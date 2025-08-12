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
import time
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
        # Ищем только задачи-заказы по шаблону 2420917 (как в рабочем примере)
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="task.getList">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            '<pageCurrent>1</pageCurrent>'
            '<pageSize>100</pageSize>'
            '<filters>'
            '  <filter>'
            '    <type>51</type>'
            '    <operator>equal</operator>'
            '    <value>2420917</value>'
            '  </filter>'
            '</filters>'
            '<fields>'
            '  <field>id</field>'
            '  <field>title</field>'
            '  <field>description</field>'
            '  <field>status</field>'
            '  <field>statusName</field>'
            '  <field>template</field>'
            '  <field>client</field>'
            '  <field>beginDateTime</field>'
            '  <field>customData</field>'  # Добавляем customData для получения номера заказа
            '</fields>'
            '</request>'
        )
        
        logger.info("Fetching ALL orders (tasks with template 2420917) for Produkty analytics...")
        
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
        all_tasks = []
        page = 1
        
        while True:
            logger.info(f"Fetching page {page} of orders...")
            
            # Обновляем номер страницы в запросе
            if page > 1:
                body = body.replace(f'<pageCurrent>{page-1}</pageCurrent>', f'<pageCurrent>{page}</pageCurrent>')
                response = requests.post(
                    planfix_utils.PLANFIX_API_URL,
                    data=body.encode('utf-8'),
                    headers=headers,
                    auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
                )
                response.raise_for_status()
                response_xml = response.text
            
            page_tasks = parse_task_list(response_xml)
            
            if not page_tasks:
                logger.info(f"No more orders found on page {page}")
                break
                
            all_tasks.extend(page_tasks)
            logger.info(f"Found {len(page_tasks)} orders on page {page}")
            
            # Если получили меньше задач чем размер страницы, значит это последняя страница
            if len(page_tasks) < 100:
                logger.info(f"Last page reached (got {len(page_tasks)} orders, expected 100)")
                break
                
            # Получаем следующую страницу
            page += 1
            
            # Добавляем задержку между страницами для избежания лимитов API
            logger.info(f"Waiting 1 second before fetching page {page}...")
            time.sleep(1)
        
        if not all_tasks:
            logger.info("No orders found with template 2420917")
            return []
        
        logger.info(f"Found {len(all_tasks)} total orders, checking for Produkty analytics in actions...")
        
        # Фильтруем задачи, которые имеют аналитику "Produkty" в действиях
        tasks_with_analytics = []
        
        for i, task in enumerate(all_tasks, 1):  # Проверяем ВСЕ заказы для получения максимального количества данных
            try:
                task_id = task['id']
                logger.info(f"Checking order {task_id} for Produkty analytics in actions... ({i}/{len(all_tasks)})")
                
                # Добавляем задержку каждые 5 задач для избежания превышения лимитов API
                if i % 5 == 0:
                    logger.info(f"  Adding delay to avoid API limits...")
                    time.sleep(2)  # 2 секунды задержки каждые 5 задач
                
                # Получаем список действий в задаче
                actions_xml = get_task_actions(task_id)
                actions = parse_task_actions(actions_xml)
                
                logger.info(f"Order {task_id} has {len(actions)} actions")
                
                # Фильтруем только действия с аналитикой Produkty (проверяем ВСЕ действия)
                actions_with_produkty = []
                logger.info(f"  Checking all {len(actions)} actions for Produkty analytics")
                
                for i, action in enumerate(actions, 1):
                    action_id = action.get('id')
                    if action_id:
                        # Показываем прогресс каждые 10 действий
                        if i % 10 == 0 or i == len(actions):
                            logger.info(f"  Progress: {i}/{len(actions)} actions checked...")
                        
                        # Получаем детали действия
                        action_details_xml = get_action_details(action_id)
                        if has_produkty_analytics_in_action(action_details_xml):
                            logger.info(f"  ✅ Action {action_id} has Produkty analytics!")
                            actions_with_produkty.append(action)
                        # Убираем логирование для действий без аналитики - ускоряем процесс
                
                if actions_with_produkty:
                    logger.info(f"Order {task_id} has {len(actions_with_produkty)} actions with Produkty analytics")
                    # Добавляем действия с аналитикой в задачу для дальнейшей обработки
                    task['actions_with_produkty'] = actions_with_produkty
                    tasks_with_analytics.append(task)
                else:
                    logger.info(f"Order {task_id} does not have Produkty analytics in any action")
                    
            except Exception as e:
                logger.warning(f"Error checking order {task_id}: {e}")
                continue
        
        logger.info(f"Found {len(tasks_with_analytics)} orders with Produkty analytics in actions")
        return tasks_with_analytics
        
    except Exception as e:
        logger.error(f"Error getting orders with analytics: {e}")
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

def get_produkty_analytics_data_by_condition(task_ids=None, page_size=100):
    """
    Получает все данные аналитики "Produkty" по условию (значительно быстрее)
    """
    try:
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        # Формируем запрос для получения всех данных аналитики Produkty
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="analitic.getDataByCondition">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            f'<analitic><id>{PRODUKTY_ANALYTIC_KEY}</id></analitic>'
            '<pageSize>100</pageSize>'
            '<pageCurrent>1</pageCurrent>'
            '</request>'
        )
        
        logger.info(f"Fetching all Produkty analytics data by condition (page size: {page_size})")
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        return response.text
        
    except Exception as e:
        logger.error(f"Error getting analytics data by condition: {e}")
        raise

def parse_task_list(xml_text):
    """
    Парсит список задач с аналитикой и извлекает номер заказа из customData
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
            
            # Извлекаем номер заказа из customData
            order_number = None
            custom_data_root = task.find('customData')
            if custom_data_root is not None:
                for cv in custom_data_root.findall('customValue'):
                    field_name = cv.findtext('field/name')
                    if field_name == "Numer zamówienia":
                        order_number = cv.findtext('value')
                        break
            
            if task_id:
                tasks.append({
                    'id': int(task_id),
                    'name': name,
                    'number': number,
                    'order_number': order_number
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

def get_task_actions(task_id):
    """
    Получает список действий в задаче через action.getList с обработкой лимитов
    """
    try:
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="action.getList">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            '<task>'
            f'  <id>{task_id}</id>'
            '</task>'
            '<pageCurrent>1</pageCurrent>'
            '<pageSize>50</pageSize>'  # Уменьшаем размер страницы для избежания лимитов
            '</request>'
        )
        
        # Добавляем задержку для избежания превышения лимитов API
        time.sleep(0.5)  # 500ms задержка между запросами
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        return response.text
        
    except Exception as e:
        logger.error(f"Error getting actions for task {task_id}: {e}")
        raise

def parse_task_actions(xml_text):
    """
    Парсит список действий из XML ответа action.getList
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        actions = []
        for action in root.findall('.//action'):
            action_id = action.findtext('id')
            if action_id:
                actions.append({
                    'id': int(action_id),
                    'text': action.findtext('text', ''),
                    'dateTime': action.findtext('dateTime', ''),
                    'type': action.findtext('type', '')
                })
        
        return actions
        
    except Exception as e:
        logger.error(f"Error parsing actions XML: {e}")
        return []

def get_action_details(action_id):
    """
    Получает детали действия через action.get с обработкой лимитов
    """
    try:
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<request method="action.get">'
            f'<account>{planfix_utils.PLANFIX_ACCOUNT}</account>'
            '<action>'
            f'  <id>{action_id}</id>'
            '</action>'
            '</request>'
        )
        
        # Добавляем задержку для избежания превышения лимитов API
        time.sleep(0.3)  # 300ms задержка между запросами
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        return response.text
        
    except Exception as e:
        logger.error(f"Error getting action details for {action_id}: {e}")
        raise

def has_produkty_analytics_in_action(xml_text):
    """
    Проверяет, есть ли аналитика "Produkty" в действии
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return False
        
        # Ищем аналитики в действии
        analytics = root.findall('.//analitic')
        if not analytics:
            analytics = root.findall('.//analytics')
        
        if not analytics:
            logger.debug("No analytics found in action")
            return False
        
        logger.info(f"Found {len(analytics)} analytics in action")
        
        # Проверяем каждую аналитику
        for analytic in analytics:
            analytic_id = analytic.findtext('id')
            analytic_name = analytic.findtext('name')
            
            if analytic_id:
                logger.info(f"  Analytic ID: {analytic_id}, Name: {analytic_name}")
                
                # Проверяем по ID (4867) или по названию ("Produkty")
                if (analytic_id == "4867" or 
                    (analytic_name and "produkty" in analytic_name.lower())):
                    logger.info(f"  ✅ Found Produkty analytics!")
                    return True
        
        logger.info("  ❌ Produkty analytics not found in action")
        return False
        
    except Exception as e:
        logger.error(f"Error checking analytics in action: {e}")
        return False

def extract_produkty_analytics_data_from_action(action_xml, task, action):
    """
    Извлекает данные аналитики "Produkty" из действия
    """
    try:
        root = ET.fromstring(action_xml)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        analytics_data = []
        
        # Ищем аналитики в действии
        analytics = root.findall('.//analitic')
        if not analytics:
            analytics = root.findall('.//analytics')
        
        if not analytics:
            logger.debug("No analytics found in action")
            return []
        
        logger.info(f"Found {len(analytics)} analytics in action")
        
        # Обрабатываем каждую аналитику
        for analytic in analytics:
            analytic_id = analytic.findtext('id')
            analytic_name = analytic.findtext('name')
            analytic_key = analytic.findtext('key')  # Ключ строки данных!
            
            if analytic_id and (analytic_id == "4867" or 
                               (analytic_name and "produkty" in analytic_name.lower())):
                
                logger.info(f"Processing Produkty analytics: ID={analytic_id}, Name={analytic_name}, Key={analytic_key}")
                
                if not analytic_key:
                    logger.warning(f"❌ No key found for analytics {analytic_id} - cannot get data")
                    continue
                
                # Получаем данные аналитики через analitic.getData используя КЛЮЧ
                logger.info(f"Requesting data for analytics key {analytic_key}...")
                analytic_data = get_analytics_data(analytic_key)
                if analytic_data:
                    logger.info(f"✅ Got analytics data for key {analytic_key}, length: {len(analytic_data)}")
                    logger.info(f"Parsing analytics data...")
                    parsed_data = parse_analytics_data(analytic_data, task, action)
                    if parsed_data:
                        analytics_data.extend(parsed_data)
                        logger.info(f"✅ Successfully parsed {len(parsed_data)} records from analytics key {analytic_key}")
                    else:
                        logger.warning(f"❌ No data parsed from analytics key {analytic_key}")
                        logger.warning(f"This means the XML structure is different than expected")
                else:
                    logger.warning(f"❌ Failed to get data for analytics key {analytic_key}")
                    logger.warning(f"This means analitic.getData returned empty or error response")
        
        return analytics_data
        
    except Exception as e:
        logger.error(f"Error extracting analytics data from action: {e}")
        return []

def get_analytics_data(analytic_key):
    """
    Получает данные аналитики через analitic.getData используя ключ строки данных
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
            f'  <key>{analytic_key}</key>'
            '</analiticKeys>'
            '</request>'
        )
        
        logger.info(f"Requesting analytics data for key {analytic_key}...")
        logger.debug(f"Request body: {body}")
        
        response = requests.post(
            planfix_utils.PLANFIX_API_URL,
            data=body.encode('utf-8'),
            headers=headers,
            auth=(planfix_utils.PLANFIX_API_KEY, planfix_utils.PLANFIX_TOKEN)
        )
        response.raise_for_status()
        response_text = response.text
        
        # Детальное логирование ответа
        logger.info(f"Analytics data response length: {len(response_text)}")
        logger.info(f"Analytics data response preview: {response_text[:1000]}...")
        
        return response_text
        
    except Exception as e:
        logger.error(f"Error getting analytics data for key {analytic_key}: {e}")
        return None

def convert_polish_number(value):
    """
    Преобразует польские числа (запятая как разделитель) в английский формат (точка как разделитель)
    для корректной записи в Supabase numeric поля
    """
    if not value or value == '':
        return None
    
    try:
        # Убираем пробелы
        value = str(value).strip()
        
        # Если пустая строка, возвращаем None
        if value == '':
            return None
        
        # Заменяем запятую на точку
        value = value.replace(',', '.')
        
        # Убираем лишние пробелы
        value = value.replace(' ', '')
        
        # Проверяем, что это число
        float(value)
        
        return value
        
    except (ValueError, TypeError):
        # Если не удалось преобразовать в число, возвращаем None
        logger.warning(f"Could not convert value '{value}' to number, setting to None")
        return None

def parse_analytics_data_by_condition(xml_text, tasks_dict):
    """
    Парсит данные аналитики из XML ответа analitic.getDataByCondition
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        logger.info(f"Parsing analytics data by condition XML...")
        logger.debug(f"XML root tag: {root.tag}")
        logger.debug(f"XML root attributes: {root.attrib}")
        
        analytics_records = []
        
        # Ищем данные аналитики
        analitic_data_nodes = root.findall('.//analiticData')
        logger.info(f"Found {len(analitic_data_nodes)} analiticData nodes")
        
        if not analitic_data_nodes:
            logger.warning("No analiticData nodes found in XML")
            return []
        
        for analitic_data in analitic_data_nodes:
            key = analitic_data.findtext('key')
            task_id = analitic_data.findtext('.//task/id')
            action_id = analitic_data.findtext('.//action/id')
            
            logger.info(f"Processing analiticData with key: {key}, task_id: {task_id}, action_id: {action_id}")
            
            # Находим задачу по ID
            task = tasks_dict.get(int(task_id)) if task_id else None
            if not task:
                logger.warning(f"Task {task_id} not found in tasks dictionary, skipping...")
                continue
            
            # Собираем данные полей
            field_data = {}
            item_data_nodes = analitic_data.findall('.//itemData')
            logger.info(f"Found {len(item_data_nodes)} itemData nodes")
            
            for item_data in item_data_nodes:
                field_id = item_data.findtext('id')
                field_name = item_data.findtext('name')
                field_value = item_data.findtext('value')
                field_value_id = item_data.findtext('valueId')
                
                logger.debug(f"Field: ID={field_id}, Name={field_name}, Value={field_value}, ValueID={field_value_id}")
                
                if field_id:
                    field_data[field_id] = {
                        'name': field_name,
                        'value': field_value,
                        'valueId': field_value_id
                    }
            
            logger.info(f"Collected field data: {field_data}")
            
            # Создаем запись для Supabase с правильными названиями полей
            record = {
                'task_id': int(task_id),
                'task_name': task.get('name', ''),
                'action_id': int(action_id) if action_id else None,
                'analytic_key': key,
                'nazwa': field_data.get('27719', {}).get('value', ''),
                'nazwa_handbook_id': field_data.get('27719', {}).get('valueId', ''),
                'cena': convert_polish_number(field_data.get('27721', {}).get('value', '')),
                'waluta': field_data.get('29133', {}).get('value', ''),
                'ilosc': convert_polish_number(field_data.get('28079', {}).get('value', '')),
                'rabat_procent': convert_polish_number(field_data.get('28109', {}).get('value', '')),
                'cena_po_rabacie': convert_polish_number(field_data.get('28111', {}).get('value', '')),
                'wartosc_netto': convert_polish_number(field_data.get('28081', {}).get('value', '')),
                'prowizja_pln': convert_polish_number(field_data.get('29311', {}).get('value', '')),
                'laczna_masa_kg': convert_polish_number(field_data.get('32907', {}).get('value', '')),
                'order_number': task.get('order_number', ''),  # Используем номер заказа из customData
                'updated_at': datetime.now(),
                'is_deleted': False
            }
            
            logger.info(f"Created record: {record}")
            analytics_records.append(record)
        
        logger.info(f"Total records parsed: {len(analytics_records)}")
        return analytics_records
        
    except Exception as e:
        logger.error(f"Error parsing analytics data by condition: {e}")
        logger.error(f"XML content: {xml_text[:500]}...")
        return []

def parse_analytics_data(xml_text, task, action):
    """
    Парсит данные аналитики из XML ответа analitic.getData (для обратной совместимости)
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        logger.info(f"Parsing analytics data XML...")
        logger.debug(f"XML root tag: {root.tag}")
        logger.debug(f"XML root attributes: {root.attrib}")
        
        analytics_records = []
        
        # Ищем данные аналитики
        analitic_data_nodes = root.findall('.//analiticData')
        logger.info(f"Found {len(analitic_data_nodes)} analiticData nodes")
        
        if not analitic_data_nodes:
            logger.warning("No analiticData nodes found in XML")
            # Попробуем альтернативные пути
            alternative_nodes = root.findall('.//analitic')
            logger.info(f"Alternative search: found {len(alternative_nodes)} analitic nodes")
            if alternative_nodes:
                for node in alternative_nodes:
                    logger.debug(f"Analytic node: {ET.tostring(node, encoding='unicode')}")
        
        for analitic_data in analitic_data_nodes:
            key = analitic_data.findtext('key')
            logger.info(f"Processing analiticData with key: {key}")
            
            # Собираем данные полей
            field_data = {}
            item_data_nodes = analitic_data.findall('.//itemData')
            logger.info(f"Found {len(item_data_nodes)} itemData nodes")
            
            for item_data in item_data_nodes:
                field_id = item_data.findtext('id')
                field_name = item_data.findtext('name')
                field_value = item_data.findtext('value')
                field_value_id = item_data.findtext('valueId')
                
                logger.debug(f"Field: ID={field_id}, Name={field_name}, Value={field_value}, ValueID={field_value_id}")
                
                if field_id:
                    field_data[field_id] = {
                        'name': field_name,
                        'value': field_value,
                        'valueId': field_value_id
                    }
            
            logger.info(f"Collected field data: {field_data}")
            
            # Создаем запись для Supabase с правильными названиями полей
            record = {
                'task_id': task['id'],
                'task_name': task.get('name', ''),
                'action_id': action.get('id'),
                'analytic_key': key,
                'nazwa': field_data.get('27719', {}).get('value', ''),
                'nazwa_handbook_id': field_data.get('27719', {}).get('valueId', ''),
                'cena': convert_polish_number(field_data.get('27721', {}).get('value', '')),
                'waluta': field_data.get('29133', {}).get('value', ''),
                'ilosc': convert_polish_number(field_data.get('28079', {}).get('value', '')),
                'rabat_procent': convert_polish_number(field_data.get('28109', {}).get('value', '')),
                'cena_po_rabacie': convert_polish_number(field_data.get('28111', {}).get('value', '')),
                'wartosc_netto': convert_polish_number(field_data.get('28081', {}).get('value', '')),
                'prowizja_pln': convert_polish_number(field_data.get('29311', {}).get('value', '')),
                'laczna_masa_kg': convert_polish_number(field_data.get('32907', {}).get('value', '')),
                'order_number': task.get('order_number', ''),  # Используем номер заказа из customData
                'updated_at': datetime.now(),
                'is_deleted': False
            }
            
            logger.info(f"Created record: {record}")
            analytics_records.append(record)
        
        logger.info(f"Total records parsed: {len(analytics_records)}")
        return analytics_records
        
    except Exception as e:
        logger.error(f"Error parsing analytics data: {e}")
        logger.error(f"XML content: {xml_text[:500]}...")
        return []

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
        
        # Получаем список заказов с аналитикой "Produkty"
        logger.info("Getting orders with Produkty analytics...")
        tasks = get_tasks_with_produkty_analytics()
        
        if not tasks:
            logger.info("No orders found with Produkty analytics")
            logger.info("This might mean:")
            logger.info("1. No orders have Produkty analytics attached")
            logger.info("2. Orders template ID might be incorrect")
            logger.info("3. Produkty analytics ID might be incorrect")
            logger.info("4. Analytics might be attached to different objects")
            return
        
        logger.info(f"Found {len(tasks)} tasks with Produkty analytics")
        logger.info("Starting data extraction process...")
        
        all_analytics_data = []
        
        # Создаем словарь задач для быстрого поиска
        tasks_dict = {task['id']: task for task in tasks}
        logger.info(f"Created tasks dictionary with {len(tasks_dict)} tasks")
        
        # Используем оптимизированный подход - получаем все данные аналитики одним запросом
        logger.info("Using optimized approach: getting all Produkty analytics data by condition...")
        
        try:
            # Получаем все данные аналитики Produkty одним запросом
            analytics_xml = get_produkty_analytics_data_by_condition()
            
            # Парсим данные аналитики
            all_analytics_data = parse_analytics_data_by_condition(analytics_xml, tasks_dict)
            
            logger.info(f"✅ Successfully extracted {len(all_analytics_data)} analytics records using optimized approach")
            
        except Exception as e:
            logger.error(f"Error using optimized approach: {e}")
            logger.info("Falling back to traditional approach...")
            
            # Fallback к традиционному подходу
            all_analytics_data = []
            for task in tasks:
                task_id = task['id']
                logger.info(f"Processing task ID: {task_id}, Name: {task.get('name', 'Unknown')}")
                
                try:
                    # Используем уже найденные действия с аналитикой Produkty
                    actions_with_produkty = task.get('actions_with_produkty', [])
                    
                    if not actions_with_produkty:
                        logger.warning(f"⚠️ Task {task_id} has no actions with Produkty analytics")
                        continue
                    
                    logger.info(f"Task {task_id} has {len(actions_with_produkty)} actions with Produkty analytics")
                    
                    # Обрабатываем только действия с аналитикой Produkty
                    found_analytics_in_task = False
                    for action in actions_with_produkty:
                        action_id = action.get('id')
                        if action_id:
                            logger.info(f"  Processing action {action_id} for Produkty analytics data...")
                            
                            # Получаем детали действия
                            action_details_xml = get_action_details(action_id)
                            
                            # Извлекаем данные аналитики "Produkty" из действия
                            analytics_data = extract_produkty_analytics_data_from_action(action_details_xml, task, action)
                            if analytics_data:
                                all_analytics_data.extend(analytics_data)
                                logger.info(f"  ✅ Extracted {len(analytics_data)} analytics records from action {action_id}")
                                found_analytics_in_task = True
                            else:
                                logger.warning(f"  ⚠️ No Produkty analytics data found in action {action_id}")
                    
                    if found_analytics_in_task:
                        logger.info(f"✅ Task {task_id} successfully processed with analytics data")
                    else:
                        logger.warning(f"⚠️ Task {task_id} processed but no analytics data extracted")
                        
                except Exception as e:
                    logger.error(f"Error processing task {task_id}: {e}")
                    continue
        
        logger.info("Data extraction process completed.")
        logger.info(f"Total analytics records collected: {len(all_analytics_data)}")
        
        if not all_analytics_data:
            logger.warning("⚠️ No analytics data to export!")
            logger.warning("This means the analytics were found but data extraction failed.")
            return
        
        logger.info(f"✅ Total analytics records to export: {len(all_analytics_data)}")
        logger.info("Starting Supabase export process...")
        
        # Получаем структуру таблицы
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{PRODUKTY_TABLE_NAME}" LIMIT 0')
            table_columns = [desc[0] for desc in cur.description]
        
        logger.info(f"Table columns: {table_columns}")
        
        # Исключаем поле 'id' из upsert (оно автоинкрементное)
        upsert_columns = [col for col in table_columns if col != 'id']
        logger.info(f"Upsert columns (excluding 'id'): {upsert_columns}")
        
        # Подготавливаем данные для upsert
        prepared_data = []
        for record in all_analytics_data:
            prepared_record = {}
            for col in upsert_columns:  # Используем колонки без 'id'
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
        logger.info("Starting Supabase upsert...")
        try:
            # Создаем составной ключ для каждой записи
            logger.info("Creating composite keys for upsert...")
            
            for record in prepared_data:
                # Создаем составной ключ: task_id_action_id_analytic_key
                record['composite_key'] = f"{record.get('task_id', '')}_{record.get('action_id', '')}_{record.get('analytic_key', '')}"
                logger.debug(f"Created composite key: {record['composite_key']}")
            
            # Используем составной ключ для upsert
            planfix_utils.upsert_data_to_supabase(
                conn,
                PRODUKTY_TABLE_NAME,
                'composite_key',  # Primary key для upsert
                upsert_columns,  # Используем колонки без 'id'
                prepared_data
            )
            logger.info(f"✅ Successfully upserted {len(prepared_data)} records to Supabase")
        except Exception as e:
            logger.error(f"❌ Error during Supabase upsert: {e}")
            raise
        
        # Получаем список всех составных ключей для пометки удаленных записей
        all_composite_keys = [item['composite_key'] for item in prepared_data if item.get('composite_key')]
        
        # Помечаем записи как удаленные
        if all_composite_keys:
            logger.info("Marking old records as deleted...")
            try:
                planfix_utils.mark_items_as_deleted_in_supabase(
                    conn,
                    PRODUKTY_TABLE_NAME,
                    'composite_key',  # Используем составной ключ
                    all_composite_keys
                )
                logger.info("✅ Successfully marked old records as deleted")
            except Exception as e:
                logger.error(f"❌ Error marking records as deleted: {e}")
        else:
            logger.warning("⚠️ No composite keys found for deletion marking")
        
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
            task_ids = set(item['task_id'] for item in all_analytics_data if item.get('task_id'))
            for task_id in list(task_ids)[:5]:  # Показываем первые 5
                print(f"Заказ (Task ID): {task_id}")

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
