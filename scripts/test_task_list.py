#!/usr/bin/env python3
"""
Тестирует API task.getList с минимальными параметрами
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.planfix_utils as planfix_utils

logger = logging.getLogger(__name__)

def test_task_list():
    """
    Тестирует API task.getList
    """
    print("🔍 Тестирование API task.getList...")
    
    try:
        # Тест 1: Без параметров
        print("\n📋 Тест 1: task.getList без параметров...")
        response = planfix_utils.make_planfix_request('task.getList', {})
        print(f"✅ Ответ получен, длина: {len(response)}")
        print(f"📄 Первые 500 символов: {response[:500]}...")
        
        # Тест 2: С базовыми параметрами
        print("\n📋 Тест 2: task.getList с базовыми параметрами...")
        params = {'pageCurrent': 1, 'pageSize': 5}
        response = planfix_utils.make_planfix_request('task.getList', params)
        print(f"✅ Ответ получен, длина: {len(response)}")
        print(f"📄 Первые 500 символов: {response[:500]}...")
        
        # Тест 3: Проверяем на ошибки
        print("\n🔍 Тест 3: Проверка на ошибки в ответе...")
        if 'status="error"' in response:
            print("❌ В ответе есть ошибка!")
            # Ищем код ошибки
            if 'code>' in response:
                start = response.find('<code>') + 6
                end = response.find('</code>')
                if start > 5 and end > start:
                    error_code = response[start:end]
                    print(f"❌ Код ошибки: {error_code}")
        else:
            print("✅ В ответе нет ошибок")
            
        # Тест 4: Парсим XML
        print("\n🔍 Тест 4: Парсинг XML...")
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(response)
            print(f"✅ XML успешно распарсен")
            print(f"📊 Корневой элемент: {root.tag}")
            print(f"📊 Атрибуты: {root.attrib}")
            
            # Ищем задачи
            tasks = root.findall('.//task')
            print(f"📋 Найдено задач: {len(tasks)}")
            
            if tasks:
                print("📋 Первая задача:")
                first_task = tasks[0]
                task_id = first_task.findtext('id')
                task_name = first_task.findtext('name')
                print(f"   ID: {task_id}")
                print(f"   Название: {task_name}")
                
        except ET.ParseError as e:
            print(f"❌ Ошибка парсинга XML: {e}")
        
        print("\n🎉 Тестирование завершено!")
        
    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    
    return True

def main():
    """
    Главная функция
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    try:
        test_task_list()
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
