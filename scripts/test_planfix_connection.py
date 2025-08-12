#!/usr/bin/env python3
"""
Простой тестовый скрипт для проверки подключения к Planfix API
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

def test_planfix_connection():
    """
    Тестирует подключение к Planfix API
    """
    print("🔍 Тестирование подключения к Planfix API...")
    
    try:
        # Проверяем переменные окружения
        print(f"✅ PLANFIX_ACCOUNT: {planfix_utils.PLANFIX_ACCOUNT}")
        print(f"✅ PLANFIX_API_KEY: {'*' * len(planfix_utils.PLANFIX_API_KEY) if planfix_utils.PLANFIX_API_KEY else 'НЕ УСТАНОВЛЕН'}")
        print(f"✅ PLANFIX_TOKEN: {'*' * len(planfix_utils.PLANFIX_TOKEN) if planfix_utils.PLANFIX_TOKEN else 'НЕ УСТАНОВЛЕН'}")
        
        # Тест 1: Получение списка аналитик
        print("\n📊 Тест 1: Получение списка аналитик...")
        params = {'pageCurrent': 1, 'pageSize': 5}
        response = planfix_utils.make_planfix_request('analitic.getList', params)
        print("✅ API analitic.getList работает")
        
        # Тест 2: Получение списка задач
        print("\n📋 Тест 2: Получение списка задач...")
        params = {'pageCurrent': 1, 'pageSize': 5}
        response = planfix_utils.make_planfix_request('task.getList', params)
        print("✅ API task.getList работает")
        
        # Тест 3: Получение данных аналитики "Produkty"
        print("\n🔍 Тест 3: Получение данных аналитики 'Produkty'...")
        params = {
            'analiticKeys': {
                'key': 4867  # ID аналитики "Produkty"
            }
        }
        response = planfix_utils.make_planfix_request('analitic.getData', params)
        print("✅ API analitic.getData работает")
        
        print("\n🎉 Все тесты прошли успешно!")
        print("Подключение к Planfix API работает корректно.")
        
    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        logger.error(f"Connection test failed: {e}", exc_info=True)
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
        test_planfix_connection()
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
