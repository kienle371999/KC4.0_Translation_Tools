import unittest
import pymongo

from modules.background_tasks.translate_file_created_by_public_request.translate_content.xlsx.main import (
    read_task_result, 
    mark_invalid_tasks,
    execute_in_batch,
    main
)

async def test_read_task_result():
    test1 = read_task_result(tasks_result = [], tasks = [], translations_history = [])
    print(test1)

    test2 = read_task_result(tasks_result = ['20220320180340_6900.xlsx'], tasks = ['20220320180340_6900.xlsx'], translations_history = ['20220320180340_6900.xlsx'])
    print(test2)

    test3 = read_task_result(tasks_result = ['20220320180340_6900 (1).xlsx'], tasks = ['20220320180340_6900 (1).xlsx'], translations_history = ['20220320180340_6900 (1).xlsx'])
    print(test3)

    test4 = read_task_result(tasks_result = ['20220320180340_69001.xlsx'], tasks = ['20220320180340_69001.xlsx'], translations_history = ['20220320180340_69001.xlsx'])
    print(test4)

    test5 = read_task_result(tasks_result = ['20220320180340_69002.xlsx'], tasks = ['20220320180340_69002.xlsx'], translations_history = ['20220320180340_69002.xlsx'])
    print(test5)

async def test_execute_in_batch():
    test1 =  execute_in_batch(valid_tasks_mapper = [], tasks_id = [], allowed_concurrent_request = [])
    print(test1)

    test2 =  execute_in_batch(valid_tasks_mapper = ['20220320180340_6900.xlsx'], tasks_id = ['20220320180340_6900.xlsx'], allowed_concurrent_request = ['20220320180340_6900.xlsx'])
    print(test2)

    test3 =  execute_in_batch(valid_tasks_mapper = ['20220320180340_6900 (1).xlsx'], tasks_id = ['20220320180340_6900 (1).xlsx'], allowed_concurrent_request = ['20220320180340_6900 (1).xlsx'])
    print(test3)
    
    test4 =  execute_in_batch(valid_tasks_mapper = ['20220320180340_69001.xlsx'], tasks_id = ['20220320180340_69001.xlsx'], allowed_concurrent_request = ['20220320180340_69001.xlsx'])
    print(test4)

    test5=  execute_in_batch(valid_tasks_mapper = ['20220320180340_69002.xlsx'], tasks_id = ['20220320180340_69002.xlsx'], allowed_concurrent_request = ['20220320180340_69002.xlsx'])
    print(test5)

async def test_mark_invalid_tasks():
    test1 = mark_invalid_tasks(invalid_tasks_mapper = [])
    print(test1)

    test2 = mark_invalid_tasks(invalid_tasks_mapper = ['20220320180340_6900.xlsx'])
    print(test2)

    test3 = mark_invalid_tasks(invalid_tasks_mapper = ['20220320180340_6900 (1).xlsx'])
    print(test3)

    test4 = mark_invalid_tasks(invalid_tasks_mapper = ['20220320180340_69001.xlsx'])
    print(test4)

    test5 = mark_invalid_tasks(invalid_tasks_mapper = ['20220320180340_69002.xlsx'])
    print(test5)

async def test_main(self):
    test = os.system('main.py')
    self.assertEqual(test, 0)



