from core.utils.common import chunk_arr
from infrastructure.configs.task import StepStatusEnum, CreatorTypeEnum
from sqlite3 import Date
from core.utils.document import check_if_paragraph_has_text, get_common_style
from core.utils.file import get_doc_paragraphs, get_full_path
from datetime import datetime
from docx import Document
from infrastructure.adapters.content_translator.main import ContentTranslator
from infrastructure.adapters.logger import Logger
from infrastructure.configs.language import LanguageEnum
from infrastructure.configs.main import GlobalConfig, get_cnf, get_mongodb_instance
from infrastructure.configs.task import TranslationTask_TranslationCompletedResultFileSchemaV1, TranslationTask_NotYetTranslatedResultFileSchemaV1, TranslationTaskNameEnum, TranslationTaskStepEnum, StepStatusEnum
from infrastructure.configs.translation_history import TranslationHistoryStatus
from infrastructure.configs.translation_task import RESULT_FILE_STATUS, AllowedFileTranslationExtensionEnum, FileTranslationTask_NotYetTranslatedResultFileSchemaV1, FileTranslationTask_TranslatingResultFileSchemaV1, FileTranslationTask_TranslationCompletedResultFileSchemaV1, get_file_translation_file_path, get_file_translation_target_file_name
from inspect import trace
from modules.background_tasks.translate_file_created_by_public_request.translate_content.docx.main import *
from modules.system_setting.database.repository import SystemSettingRepository
from modules.translation_request.database.translation_history.repository import TranslationHistoryRepository, TranslationHistoryEntity, TranslationHistoryProps
from modules.translation_request.database.translation_request.repository import TranslationRequestRepository, TranslationRequestEntity, TranslationRequestProps
from modules.translation_request.database.translation_request_result.repository import TranslationRequestResultRepository, TranslationRequestResultEntity, TranslationRequestResultProps
from typing import List
from core.value_objects.id import ID
from uuid import UUID
import aiohttp
import asyncio
import io
import json
import pickle
import pymongo
import random
import traceback

config: GlobalConfig = get_cnf()
db_instance = get_mongodb_instance()

LIMIT_NUM_CHAR_TRANSLATE_REQUEST = 3000

translation_request_repository = TranslationRequestRepository()
translation_request_result_repository = TranslationRequestResultRepository()
translation_history_repository = TranslationHistoryRepository()
system_setting_repository = SystemSettingRepository()

contentTranslator = ContentTranslator()

logger = Logger(
    'Task: translate_file_created_by_public_request'
)


async def test_read_task_result():

    print("===>>>>>>> Test read_task_result <<<<<<<<<<<<===")
    print("TEST 1")
    try:
        valid_tasks_mapper, invalid_tasks_mapper = await read_task_result([], [], [])
        print("=== Test read_task_result in testcase 1: TRUE  ===")
    except Exception as e:
        print(e)
        print("=== Test read_task_result in testcase 1: FALSE ===")

    print("TEST 2")
    try:
        # print("task init: ")
        new_request = TranslationRequestEntity(
            TranslationRequestProps(
                creator_id=ID("660c1f23-6d26-41e8-a5dd-736c44248d0e"),
                creator_type="end_user",
                task_name='private_plain_text_translation',
                step_status="closed",
                current_step="detecting_language",
                create_at=Date(2022, 3, 24),
                _cls="LanguageDetectionRequestOrmEntity"
            )
        )
        tran = TranslationRequestResultEntity(
            TranslationRequestResultProps(
                task_id=new_request.id,
                step=new_request.props.current_step,
            )
        )
        tasks = [tran]

        # print("task_res init: ")
        new_request_res = TranslationRequestEntity(
            TranslationRequestProps(
                id=ID("660c1f23-6d26-41e8-a5dd-736c44248d0e"),
                creator_id=ID("377b5a56-51bd-40e7-8b52-73060e5f8c32"),
                creator_type="end_user",
                task_name='private_plain_text_translation',
                step_status="closed",
                current_step="detecting_language",
                create_at=Date(2022, 3, 24),
                update_at=Date(2022, 3, 24),
                _cls="LanguageDetectionRequestOrmEntity",
                file_path="1648110413183__660c1f23-6d26-41e8-a5dd-736c44248d0e.json"
            )
        )
        task_res = TranslationRequestResultEntity(
            TranslationRequestResultProps(
                task_id=new_request_res.id,
                step=new_request_res.props.current_step
            )
        )
        # print(task_res)
        tasks_result = [task_res]

        # print("history init ")
        new_request_his = TranslationRequestEntity(
            TranslationRequestProps(
                creator_id=ID("76b76d60-2682-4c53-b092-c8262a353dba"),
                creator_type=CreatorTypeEnum.end_user.value,
                task_name=TranslationTaskNameEnum.private_plain_text_translation.value,
                step_status=StepStatusEnum.not_yet_processed.value,
                current_step=TranslationTaskStepEnum.detecting_language.value
            )
        )
        tran_history = TranslationHistoryEntity(
            TranslationHistoryProps(
                creator_id=new_request_his.props.creator_id,
                task_id=new_request_his.id,
                translation_type=new_request_his.props.task_name,
                status=TranslationHistoryStatus.translating.value,
                file_path="1648110429829__89aa81e5-6dca-4589-afc4-af2f56d9cb9f.json"
            )
        )
        translations_history = [tran_history]
        print("INIT ARGUMENT SUCCESS")

        valid_tasks_mapper, invalid_tasks_mapper = await read_task_result(
            tasks=tasks,
            tasks_result=tasks_result,
            translations_history=translations_history
        )
        print("=== Test read_task_result: TRUE ===")
        # print("=== VALID TASKS MAPPER ===\n")
        # print(valid_tasks_mapper + "\n")
        # print("=== INVALID TASKS MAPPER ===\n")
        # print(invalid_tasks_mapper)
        # print("=== Test read_task_result: TRUE  ===")
    except Exception as e:
        print(e)
        print("=== Test read_task_result: FALSE ===")


async def test_mark_invalid_tasks():
    print("---->>>>>>> Test mark_invalid_tasks <<<<<<<<<<<<----")

    print("TEST1: ")
    try:
        print("INIT ARGUMENT SUCCESS")
        invalid_tasks_mapper = {}
        await mark_invalid_tasks(invalid_tasks_mapper)
        print("---- Test mark_invalid_tasks in testcase 1: TRUE ----")
    except Exception as e:
        print(e)
        print("---- Test mark_invalid_tasks in testcase 1: FALSE ----")

    print("TEST 2: ")
    try:
        # print("task init: ")
        new_request = TranslationRequestEntity(
            TranslationRequestProps(
                creator_id=ID("660c1f23-6d26-41e8-a5dd-736c44248d0e"),
                creator_type="end_user",
                task_name='public_plain_text_translation',
                step_status="closed",
                current_step="detecting_language",
                create_at=Date(2022, 3, 31),
                _cls="LanguageDetectionRequestOrmEntity"
            )
        )
        tran = TranslationRequestResultEntity(
            TranslationRequestResultProps(
                task_id=new_request.id,
                step=new_request.props.current_step,
            )
        )
        tasks = [tran]

        # print("task_res init: ")
        new_request_res = TranslationRequestEntity(
            TranslationRequestProps(
                id=ID("660c1f23-6d26-41e8-a5dd-736c44248d0e"),
                creator_id=ID("377b5a56-51bd-40e7-8b52-73060e5f8c32"),
                creator_type="end_user",
                task_name='public_plain_text_translation',
                step_status="closed",
                current_step="detecting_language",
                create_at=Date(2022, 3, 31),
                update_at=Date(2022, 3, 31),
                _cls="LanguageDetectionRequestOrmEntity",
                file_path="1648110413183__660c1f23-6d26-41e8-a5dd-736c44248d0e.json"
            )
        )
        task_res = TranslationRequestResultEntity(
            TranslationRequestResultProps(
                task_id=new_request_res.id,
                step=new_request_res.props.current_step
            )
        )
        # print(task_res)
        tasks_result = [task_res]

        # print("history init ")
        new_request_his = TranslationRequestEntity(
            TranslationRequestProps(
                creator_id=ID("76b76d60-2682-4c53-b092-c8262a353dba"),
                creator_type=CreatorTypeEnum.end_user.value,
                task_name=TranslationTaskNameEnum.public_plain_text_translation.value,
                step_status=StepStatusEnum.not_yet_processed.value,
                current_step=TranslationTaskStepEnum.detecting_language.value
            )
        )
        tran_history = TranslationHistoryEntity(
            TranslationHistoryProps(
                creator_id=new_request_his.props.creator_id,
                task_id=new_request_his.id,
                translation_type=new_request_his.props.task_name,
                status=TranslationHistoryStatus.translating.value,
                file_path="1648110429829__89aa81e5-6dca-4589-afc4-af2f56d9cb9f.json"
            )
        )
        translations_history = [tran_history]

        print("INIT ARGUMENT SUCCESS")
        invalid_tasks_mapper = {0: {
            'task_result': tasks_result,
            'trans_history': translations_history,
            'task': tasks
        }}
        await mark_invalid_tasks(invalid_tasks_mapper)
        print("---- Test mark_invalid_tasks in testcase 1: TRUE ----")
    except Exception as e:
        print(e)
        print("---- Test mark_invalid_tasks in testcase 2: FALSE ----")


async def test_execute_in_batch():
    print("---->>>>>>> Test execute_in_batch <<<<<<<<<<<<----")
    from modules.system_setting.database.repository import SystemSettingRepository

    system_setting_repository = SystemSettingRepository()
    system_setting = await system_setting_repository.find_one({})

    ALLOWED_CONCURRENT_REQUEST = system_setting.props.translation_api_allowed_concurrent_req

    print("TEST 1: ")
    try:
        print("INIT ARGUMENT SUCCESS")
        valid_tasks_mapper = []
        await execute_in_batch(valid_tasks_mapper, [], ALLOWED_CONCURRENT_REQUEST)
        print("Test execute_in_batch in testcase 1: TRUE")
    except Exception as e:
        print(e)
        print("Test execute_in_batch in testcase 1: FALSE")

    print("TEST 2: ")
    try:
        await execute_in_batch([], [], None)
        print("Test execute_in_batch in test case 2: TRUE")
    except Exception as e:
        print(e)
        print("Test execute_in_batch in test case 2: FALSE")

    # try:
    #     tasks: List[TranslationRequestEntity]
    #     tasks_result: List[TranslationRequestResultEntity]
    #     translations_history: List[TranslationHistoryEntity]

    # except Exception as e:
    #     print(e)
    #     return


async def test_main():
    print("---- Test main ----")
    await main()
    try:
        await main()
        print('Test translate_plain_text_created_by_public_request TRUE')
    except Exception as e:
        print(e)
        print('Test translate_plain_text_created_by_public_request FALSE')

async def test_all():
    await test_read_task_result()
    await test_mark_invalid_tasks()
    await test_execute_in_batch()
    await test_main() 