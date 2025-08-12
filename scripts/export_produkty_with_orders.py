#!/usr/bin/env python3
"""
Скрипт для экспорта аналитики "Produkty" из Planfix в Supabase с привязкой к заказам.
Получает данные аналитики и связывает их с задачами (заказами).
"""

import os
import sys
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.planfix_utils as planfix_utils

logger = logging.getLogger(__name__)

# Конфигурация
PRODUKTY_ANALYTIC_KEY = 4867  # ID аналитики "Produkty"
PRODUKTY_TABLE_NAME = "planfix_analytics_produkty"

def get_tasks_with_produkty_analytics():
    """
    Получает список задач (заказов) с прикрепленной аналитикой "Produkty"
    """
    try:
        # Используем правильный подход с ручным формированием XML
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="task.getList">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            '<pageCurrent>1</pageCurrent>'
            '<pageSize>50</pageSize>'
            '<fields>'
            '  <field>id</field>'
            '  <field>title</field>'
            '  <field>description</field>'
            '  <field>status</field>'
            '  <field>statusName</field>'
            '  <field>template</field>'
            '  <field>client</field>'
            '  <field>beginDateTime</field>'
            '</fields>'
            '</request>'
        )
        
        logger.info("Fetching tasks list with proper XML format...")
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        response_xml = response.text
        
        # Логируем ответ для отладки
        logger.info(f"API response length: {len(response_xml)}")
        logger.debug(f"API response preview: {response_xml[:500]}...")
        
        # Парсим список задач
        tasks = parse_task_list(response_xml)
        
        if not tasks:
            logger.info("No tasks found")
            return []
        
        logger.info(f"Found {len(tasks)} tasks, checking for Produkty analytics...")
        
        # Фильтруем задачи, которые имеют аналитику "Produkty"
        tasks_with_analytics = []
        
        for task in tasks[:10]:  # Проверяем первые 10 задач для экономии API вызовов
            try:
                task_id = task['id']
                logger.info(f"Checking task {task_id} for Produkty analytics...")
                
                # Получаем детали задачи
                task_details_xml = get_task_details(task_id)
                task_info = parse_task_details(task_details_xml)
                
                # Логируем детали задачи для отладки
                logger.info(f"Task {task_id} details: {task_info}")
                
                # Проверяем, есть ли аналитика "Produkty"
                if has_produkty_analytics(task_details_xml):
                    logger.info(f"Task {task_id} has Produkty analytics")
                    tasks_with_analytics.append(task)
                else:
                    logger.info(f"Task {task_id} does not have Produkty analytics")
                    # Логируем XML для отладки
                    logger.debug(f"Task {task_id} XML preview: {task_details_xml[:1000]}...")
                    
            except Exception as e:
                logger.warning(f"Error checking task {task_id}: {e}")
                continue
        
        logger.info(f"Found {len(tasks_with_analytics)} tasks with Produkty analytics")
        return tasks_with_analytics
        
    except Exception as e:
        logger.error(f"Error getting tasks with analytics: {e}")
        raise

def has_produkty_analytics(task_xml):
    """
    Проверяет, есть ли в задаче аналитика "Produkty"
    """
    try:
        root = ET.fromstring(task_xml)
        
        # Логируем структуру XML для отладки
        logger.info(f"XML root tag: {root.tag}")
        logger.info(f"XML root attributes: {root.attrib}")
        
        # Ищем аналитики в разных возможных местах
        analytics = root.findall('.//analitic')
        if not analytics:
            logger.info("No analytics found with './/analitic' path")
            # Попробуем другие пути
            analytics = root.findall('.//analytics')
            if analytics:
                logger.info(f"Found analytics with './/analytics' path: {len(analytics)}")
            else:
                logger.info("No analytics found with './/analytics' path either")
        
        logger.info(f"Found {len(analytics)} analytics in task")
        
        for i, analitic in enumerate(analytics):
            analitic_id = analitic.findtext('id')
            analitic_name = analitic.findtext('name')
            logger.info(f"  Analytics {i+1}: ID={analitic_id}, Name={analitic_name}")
            
            if analitic_id and int(analitic_id) == PRODUKTY_ANALYTIC_KEY:
                logger.info(f"  ✅ Found Produkty analytics with ID {analitic_id}")
                return True
        
        logger.info(f"  ❌ Produkty analytics (ID {PRODUKTY_ANALYTIC_KEY}) not found")
        return False
        
    except Exception as e:
        logger.warning(f"Error parsing task XML for analytics check: {e}")
        return False

def get_task_details(task_id):
    """
    Получает детали задачи (заказа) с аналитикой
    """
    try:
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="task.get">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            f'<task>'
            f'<id>{task_id}</id>'
            '</task>'
            '</request>'
        )
        
        logger.info(f"Fetching task details for ID: {task_id}")
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        return response.text
        
    except Exception as e:
        logger.error(f"Error getting task details for ID {task_id}: {e}")
        raise

def get_produkty_analytics_data(task_id):
    """
    Получает данные аналитики "Produkty" для конкретной задачи
    """
    try:
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="analitic.getData">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            '<analiticKeys>'
            f'<key>{PRODUKTY_ANALYTIC_KEY}</key>'
            '</analiticKeys>'
            f'<taskId>{task_id}</taskId>'
            '</request>'
        )
        
        logger.info(f"Fetching Produkty analytics data for task ID: {task_id}")
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        return response.text
        
    except Exception as e:
        logger.error(f"Error getting analytics data for task ID {task_id}: {e}")
        raise

def parse_task_list(xml_text):
    """
    Парсит список задач с аналитикой
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        tasks = []
        for task in root.findall('.//task'):
            task_id = task.findtext('id')
            name = task.findtext('name')
            number = task.findtext('number')
            
            if task_id:
                tasks.append({
                    'id': int(task_id),
                    'name': name,
                    'number': number
                })
        
        return tasks
        
    except ET.ParseError as e:
        logger.error(f"XML ParseError: {e}")
        raise

def parse_task_details(xml_text):
    """
    Парсит детали задачи
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return {}
        
        task_info = {}
        
        # Получаем номер заказа
        number = root.findtext('.//task/number')
        if number:
            task_info['order_number'] = number
        
        # Получаем название задачи
        name = root.findtext('.//task/name')
        if name:
            task_info['task_name'] = name
        
        return task_info
        
    except ET.ParseError as e:
        logger.error(f"XML ParseError: {e}")
        raise

def parse_produkty_analytics_data(xml_text, task_id, order_number):
    """
    Парсит данные аналитики "Produkty" для конкретной задачи
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        analytics_data = []
        
        # Парсим analiticDatas
        for analitic_data in root.findall('.//analiticData'):
            key = analitic_data.findtext('key')
            if key is None:
                continue
            
            # Парсим itemData для каждой записи аналитики
            for item_data in analitic_data.findall('.//itemData'):
                item_id = item_data.findtext('id')
                name = item_data.findtext('name')
                value = item_data.findtext('value')
                value_id = item_data.findtext('valueId')
                
                # Создаем уникальный ID для записи
                record_id = f"{key}_{task_id}_{item_id}" if item_id else f"{key}_{task_id}_{len(analytics_data)}"
                
                # Создаем базовую структуру записи
                record = {
                    'id': record_id,
                    'planfix_analytic_id': int(key) if key else None,
                    'planfix_item_id': int(item_id) if item_id else None,
                    'task_id': task_id,
                    'order_number': order_number,
                    'updated_at': datetime.now(),
                    'is_deleted': False
                }
                
                # Добавляем поля аналитики на основе названия
                if name:
                    clean_name = clean_field_name(name)
                    record[clean_name] = value
                    
                    # Если есть value_id, добавляем отдельную колонку
                    if value_id:
                        record[f"{clean_name}_handbook_id"] = value_id
                
                analytics_data.append(record)
        
        return analytics_data
        
    except ET.ParseError as e:
        logger.error(f"XML ParseError: {e}")
        raise

def clean_field_name(field_name):
    """
    Очищает название поля для использования в SQL
    """
    # Заменяем польские символы и специальные символы
    replacements = {
        'ą': 'a', 'ć': 'c', 'ę': 'e', 'ł': 'l', 'ń': 'n', 'ó': 'o', 'ś': 's', 'ź': 'z', 'ż': 'z',
        'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 'Ł': 'L', 'Ń': 'N', 'Ó': 'O', 'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z',
        ' ': '_', '-': '_', '.': '_', ',': '_', '%': 'procent'
    }
    
    clean_name = field_name
    for old, new in replacements.items():
        clean_name = clean_name.replace(old, new)
    
    # Убираем все неалфавитно-цифровые символы
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
    
    # Приводим к нижнему регистру
    return clean_name.lower()

def export_produkty_with_orders():
    """
    Главная функция экспорта аналитики "Produkty" с привязкой к заказам
    """
    logger.info("--- Starting Produkty Analytics Export with Orders ---")
    
    # Проверяем обязательные переменные окружения
    planfix_utils.check_required_env_vars({
        'PLANFIX_API_KEY': planfix_utils.PLANFIX_API_KEY,
        'PLANFIX_TOKEN': planfix_utils.PLANFIX_TOKEN,
        'PLANFIX_ACCOUNT': planfix_utils.PLANFIX_ACCOUNT,
    })

    conn = None
    try:
        # Подключаемся к Supabase
        conn = planfix_utils.get_supabase_connection()
        
        # Получаем список задач с аналитикой "Produkty"
        logger.info("Getting tasks with Produkty analytics...")
        tasks = get_tasks_with_produkty_analytics()
        
        if not tasks:
            logger.info("No tasks found with Produkty analytics")
            logger.info("This might mean:")
            logger.info("1. No tasks have Produkty analytics attached")
            logger.info("2. All tasks are in different statuses")
            logger.info("3. Produkty analytics ID might be incorrect")
            return
        
        logger.info(f"Found {len(tasks)} tasks with Produkty analytics")
        
        all_analytics_data = []
        
        # Обрабатываем каждую задачу
        for task in tasks:
            task_id = task['id']
            logger.info(f"Processing task ID: {task_id}, Name: {task['name']}")
            
            try:
                # Получаем детали задачи
                task_details_xml = get_task_details(task_id)
                task_info = parse_task_details(task_details_xml)
                
                order_number = task_info.get('order_number', f"TASK_{task_id}")
                
                # Получаем данные аналитики для задачи
                analytics_xml = get_produkty_analytics_data(task_id)
                analytics_data = parse_produkty_analytics_data(analytics_xml, task_id, order_number)
                
                if analytics_data:
                    logger.info(f"Found {len(analytics_data)} analytics records for task {task_id}")
                    all_analytics_data.extend(analytics_data)
                else:
                    logger.warning(f"No analytics data found for task {task_id}")
                
            except Exception as e:
                logger.error(f"Error processing task {task_id}: {e}")
                continue
        
        if not all_analytics_data:
            logger.info("No analytics data to export")
            return
        
        logger.info(f"Total analytics records to export: {len(all_analytics_data)}")
        
        # Получаем структуру таблицы
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{PRODUKTY_TABLE_NAME}" LIMIT 0')
            table_columns = [desc[0] for desc in cur.description]
        
        logger.info(f"Table columns: {table_columns}")
        
        # Подготавливаем данные для upsert
        prepared_data = []
        for record in all_analytics_data:
            prepared_record = {}
            for col in table_columns:
                if col in record:
                    prepared_record[col] = record[col]
                else:
                    # Устанавливаем значения по умолчанию
                    if col == 'is_deleted':
                        prepared_record[col] = False
                    elif col == 'updated_at':
                        prepared_record[col] = datetime.now()
                    else:
                        prepared_record[col] = None
            prepared_data.append(prepared_record)
        
        # Обновляем данные в Supabase
        planfix_utils.upsert_data_to_supabase(
            conn,
            PRODUKTY_TABLE_NAME,
            'id',
            table_columns,
            prepared_data
        )
        
        # Получаем список всех ID для пометки удаленных записей
        all_ids = [item['id'] for item in prepared_data if item.get('id')]
        
        # Помечаем записи как удаленные
        planfix_utils.mark_items_as_deleted_in_supabase(
            conn,
            PRODUKTY_TABLE_NAME,
            'id',
            all_ids
        )
        
        logger.info("--- Produkty Analytics Export with Orders finished successfully ---")
        
        # Выводим статистику
        print(f"\n=== Статистика экспорта ===")
        print(f"Обработано задач: {len(tasks)}")
        print(f"Экспортировано записей: {len(all_analytics_data)}")
        print(f"Таблица: {PRODUKTY_TABLE_NAME}")
        print(f"Ключ аналитики: {PRODUKTY_ANALYTIC_KEY}")
        print(f"Время экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Показываем примеры заказов
        if all_analytics_data:
            print(f"\n=== Примеры заказов ===")
            order_numbers = set(item['order_number'] for item in all_analytics_data if item.get('order_number'))
            for order_num in list(order_numbers)[:5]:  # Показываем первые 5
                print(f"Заказ: {order_num}")

    except Exception as e:
        logger.critical(f"An error occurred during export: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Supabase connection closed.")

def main():
    """
    Точка входа в программу
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    try:
        export_produkty_with_orders()
    except KeyboardInterrupt:
        print("\nЭкспорт прерван пользователем")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
