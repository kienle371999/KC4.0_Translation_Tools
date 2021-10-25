from datetime import datetime
import logging
from infrastructure.adapters.speech_recognitor.main import SpeechRecognitor
from infrastructure.configs.language import LanguageEnum
from infrastructure.configs.speech_recognition_history import SpeechRecognitionHistoryStatus
from core.utils.common import chunk_arr


from typing import List, Union
from uuid import UUID

from infrastructure.configs.main import GlobalConfig, get_cnf, get_mongodb_instance
from infrastructure.configs.speech_recognition_task import SPEECH_RECOGNITION_RESULT_FILE_STATUS, SpeechRecognitionTask_ConvertingResultFileSchemaV1, SpeechRecognitionTask_NotYetConvertedResultFileSchemaV1
from infrastructure.configs.task import (
    TranslationTask_TranslationCompletedResultFileSchemaV1, 
    TranslationTask_NotYetTranslatedResultFileSchemaV1, 
    TranslationTaskNameEnum, 
    SpeechRecognitionTaskStepEnum, 
    StepStatusEnum
)

from infrastructure.adapters.content_translator.main import ContentTranslator 


from infrastructure.configs.task import (
    SpeechRecognitionTaskStepEnum,
    SpeechRecognitionTaskNameEnum,
)

import asyncio
import aiohttp

from infrastructure.adapters.logger import Logger
from modules.speech_recognition_request.database.speech_recognition_history.repository import SpeechRecognitionHistoryRepository
from modules.speech_recognition_request.database.speech_recognition_request.repository import SpeechRecognitionRequestRepository
from modules.speech_recognition_request.database.speech_recognition_request_result.repository import SpeechRecognitionRequestResultRepository
from modules.speech_recognition_request.domain.entities.speech_recognition_request import SpeechRecognitionRequestEntity
from modules.speech_recognition_request.domain.entities.speech_recognition_request_result import SpeechRecognitionRequestResultEntity
from modules.translation_request.domain.entities.translation_history import TranslationHistoryEntity

config: GlobalConfig = get_cnf()
db_instance = get_mongodb_instance()

PUBLIC_LANGUAGE_DETECTION_API_CONF = config.PUBLIC_LANGUAGE_DETECTION_API
ALLOWED_CONCURRENT_REQUEST = PUBLIC_LANGUAGE_DETECTION_API_CONF.ALLOWED_CONCURRENT_REQUEST

speech_recognition_request_repository = SpeechRecognitionRequestRepository()
speech_recognition_request_result_repository = SpeechRecognitionRequestResultRepository()
speech_recognition_history_repository = SpeechRecognitionHistoryRepository()

speechRecognitor = SpeechRecognitor()

logger = Logger('Task: recognize_speech_in_public_request.recognize_speech')

async def read_task_result(
    tasks_result: List[SpeechRecognitionRequestResultEntity], 
    tasks: List[SpeechRecognitionRequestEntity],
    speech_recognitions_history: List[TranslationHistoryEntity]
):
    
    valid_tasks_mapper = {}

    task_id_1 = list(map(lambda t: t.id.value, tasks))
    task_id_2 = list(map(lambda ts: ts.props.task_id.value, tasks_result))
    task_id_3 = list(map(lambda th: th.props.task_id.value, speech_recognitions_history))

    intersection_tasks_id = list(set(task_id_1) & set(task_id_2) & set(task_id_3))
    
    for task_id in intersection_tasks_id:

        task = list(filter(lambda ts: ts.id.value == task_id, tasks))[0]
        task_result = list(filter(lambda ts: ts.props.task_id.value == task_id, tasks_result))[0]
        recognize_history = list(filter(lambda ts: ts.props.task_id.value == task_id, speech_recognitions_history))[0]

        try: 
            data = await task_result.read_data_from_file()
            
            if data['status'] == SPEECH_RECOGNITION_RESULT_FILE_STATUS['not_yet_converted']:

                valid_tasks_mapper[task_id] = {
                    'task_result_content': data,
                    'task_result': task_result,
                    'recognize_history': recognize_history,
                    'task': task
                }

        except Exception as e:
            logger.error(e)

            print(e)

    valid_tasks_id = valid_tasks_mapper.keys()

    invalid_tasks = list(filter(lambda t: t.id.value not in valid_tasks_id, tasks))

    invalid_tasks_id = list(map(lambda t: t.id.value, invalid_tasks))

    invalid_tasks_mapper = {}
    
    for task_id in invalid_tasks_id:

        task = list(filter(lambda ts: ts.id.value == task_id, tasks))[0]
        task_result = list(filter(lambda ts: ts.props.task_id.value == task_id, tasks_result))[0]
        recognize_history = list(filter(lambda ts: ts.props.task_id.value == task_id, speech_recognitions_history))[0]

        invalid_tasks_mapper[task_id] = {
            'task_result': task_result,
            'recognize_history': recognize_history,
            'task': task
        }

    return valid_tasks_mapper, invalid_tasks_mapper

async def mark_invalid_tasks(invalid_tasks_mapper):

    result = []
    
    async with db_instance.session() as session:
        async with session.start_transaction():

            update_request = []
            
            for task_id in invalid_tasks_mapper.keys():

                task_result = invalid_tasks_mapper[task_id]['task_result'],
                recognize_history = invalid_tasks_mapper[task_id]['recognize_history'],
                task = invalid_tasks_mapper[task_id]['task']

                if isinstance(task_result, tuple):
                    task_result = task_result[0]

                if isinstance(recognize_history, tuple):
                    recognize_history = recognize_history[0]
                    
                update_request.append(
                    speech_recognition_request_repository.update(
                        task, 
                        dict(step_status=StepStatusEnum.cancelled.value),
                        conditions={}
                    )
                )
                
                update_request.append(
                    speech_recognition_history_repository.update(
                        recognize_history, 
                        dict(
                            status=SpeechRecognitionHistoryStatus.cancelled.value
                        )
                    )
                )

            result = await asyncio.gather(*update_request)

    return result

async def main():

    logger.debug(
        msg=f'New task recognize_speech_in_public_request.recognize_speech run in {datetime.now()}'
    )

    print(f'New task recognize_speech_in_public_request.recognize_speech run in {datetime.now()}')
    
    try:
        tasks = await speech_recognition_request_repository.find_many(
            params=dict(
                task_name={
                    "$in":[SpeechRecognitionTaskNameEnum.public_speech_recognition.value, SpeechRecognitionTaskNameEnum.public_speech_translation.value,]
                },
                current_step=SpeechRecognitionTaskStepEnum.converting_speech.value,
                step_status=StepStatusEnum.not_yet_processed.value,
                expired_date={
                    "$gt": datetime.now()
                }
            ),
            limit=ALLOWED_CONCURRENT_REQUEST * 10
        )

        tasks_id = list(map(lambda task: task.id.value, tasks))

        if len(tasks_id) == 0: 
            logger.debug(
                msg=f'An task recognize_speech_in_public_request.recognize_speech end in {datetime.now()}\n'
            )
            print(f'An task recognize_speech_in_public_request.recognize_speech end in {datetime.now()}\n')
            return

        tasks_result_and_recognition_history_req = [
            speech_recognition_request_result_repository.find_many(
                params=dict(
                    task_id={
                        '$in': list(map(lambda t: UUID(t), tasks_id))
                    },
                    step=SpeechRecognitionTaskStepEnum.converting_speech.value
                )
            ),
            speech_recognition_history_repository.find_many(
                params=dict(
                    task_id={
                        '$in': list(map(lambda t: UUID(t), tasks_id))
                    }
                )
            )
        ]

        tasks_result, speech_recognitions_history = await asyncio.gather(*tasks_result_and_recognition_history_req)

        valid_tasks_mapper, invalid_tasks_mapper = await read_task_result(
            tasks=tasks, 
            tasks_result=tasks_result,
            speech_recognitions_history=speech_recognitions_history
        )

        await mark_invalid_tasks(invalid_tasks_mapper)

        valid_tasks_id = list(map(lambda t: t.id.value, tasks))

        chunked_tasks_id = list(chunk_arr(valid_tasks_id, ALLOWED_CONCURRENT_REQUEST))
        
        for chunk in chunked_tasks_id:
            
            await execute_in_batch(valid_tasks_mapper, chunk)

    except Exception as e:
        logger.error(e)
        
        print(e)

    logger.debug(
        msg=f'An task recognize_speech_in_public_request.recognize_speech end in {datetime.now()}\n'
    )

    print(f'An task recognize_speech_in_public_request.recognize_speech end in {datetime.now()}\n')
            

async def execute_in_batch(valid_tasks_mapper, tasks_id):

    loop = asyncio.get_event_loop()

    connector = aiohttp.TCPConnector(limit=ALLOWED_CONCURRENT_REQUEST)

    async with aiohttp.ClientSession(connector=connector, loop=loop) as session:

        api_requests = []

        for task_id in tasks_id:

            source_lang = valid_tasks_mapper[task_id]['task_result_content']['source_lang']
            source_file_full_path = valid_tasks_mapper[task_id]['task_result_content']['source_file_full_path']

            api_requests.append(
                    speechRecognitor.send_request(
                        source_file_full_path=source_file_full_path,
                        source_lang=source_lang,
                        session=session,
                    )
                )
            
        api_results = await asyncio.gather(*api_requests)

        async with db_instance.session() as session:

            async with session.start_transaction():
                
                update_request = []
                
                for task_id, api_result in zip(tasks_id, api_results):

                    task_result = valid_tasks_mapper[task_id]['task_result'],
                    recognize_history = valid_tasks_mapper[task_id]['recognize_history'],
                    task = valid_tasks_mapper[task_id]['task']
                    task_result_content = valid_tasks_mapper[task_id]['task_result_content']

                    if task_result_content['task_name'] == SpeechRecognitionTaskNameEnum.public_speech_recognition.value:
                    
                        new_saved_content = SpeechRecognitionTask_ConvertingResultFileSchemaV1(
                            source_file_full_path=task_result_content['source_file_full_path'],
                            source_lang=task_result_content['source_lang'],
                            job_id=api_result.id,
                            task_name=SpeechRecognitionTaskNameEnum.public_speech_recognition.value
                        )

                    elif task_result_content['task_name'] == SpeechRecognitionTaskNameEnum.public_speech_translation.value:

                        new_saved_content = SpeechRecognitionTask_ConvertingResultFileSchemaV1(
                            source_file_full_path=task_result_content['source_file_full_path'],
                            source_lang=task_result_content['source_lang'],
                            target_lang=task_result_content['target_lang'],
                            job_id=api_result.id,
                            task_name=SpeechRecognitionTaskNameEnum.public_speech_translation.value
                        )

                    if isinstance(task_result, tuple):
                        task_result = task_result[0]

                    if isinstance(recognize_history, tuple):
                        recognize_history = recognize_history[0]

                    update_request.append(
                        speech_recognition_request_repository.update(
                            task, 
                            dict(
                                step_status=StepStatusEnum.in_progress.value,
                                current_step=SpeechRecognitionTaskStepEnum.converting_speech.value
                            )
                        )
                    )
                    
                    update_request.append(
                        speech_recognition_history_repository.update(
                            recognize_history, 
                            dict(
                                status=SpeechRecognitionHistoryStatus.converting.value
                            )
                        )
                    )

                    update_request.append(
                        task_result.save_request_result_to_file(
                            content=new_saved_content.json()
                        )
                    )
                
                await asyncio.gather(*update_request)