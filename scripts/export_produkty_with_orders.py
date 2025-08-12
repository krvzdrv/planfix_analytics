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
from datetime import datetime

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
            '<pageSize>50</pageSize>'
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
            '</fields>'
            '</request>'
        )
        
        logger.info("Fetching orders (tasks with template 2420917) for Produkty analytics...")
        
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
            logger.info("No orders found with template 2420917")
            return []
        
        logger.info(f"Found {len(tasks)} orders, checking for Produkty analytics in actions...")
        
        # Фильтруем задачи, которые имеют аналитику "Produkty" в действиях
        tasks_with_analytics = []
        
        for task in tasks[:10]:  # Проверяем первые 10 заказов для экономии API вызовов
            try:
                task_id = task['id']
                logger.info(f"Checking order {task_id} for Produkty analytics in actions...")
                
                # Получаем список действий в задаче
                actions_xml = get_task_actions(task_id)
                actions = parse_task_actions(actions_xml)
                
                logger.info(f"Order {task_id} has {len(actions)} actions")
                
                # Проверяем каждое действие на наличие аналитики "Produkty"
                has_produkty = False
                for action in actions:
                    action_id = action.get('id')
                    if action_id:
                        logger.info(f"  Checking action {action_id} for Produkty analytics...")
                        
                        # Получаем детали действия
                        action_details_xml = get_action_details(action_id)
                        if has_produkty_analytics_in_action(action_details_xml):
                            logger.info(f"  ✅ Action {action_id} has Produkty analytics!")
                            has_produkty = True
                            break
                        else:
                            logger.info(f"  ❌ Action {action_id} does not have Produkty analytics")
                
                if has_produkty:
                    logger.info(f"Order {task_id} has Produkty analytics in actions")
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

def get_task_actions(task_id):
    """
    Получает список действий в задаче через action.getList
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
            '<pageSize>100</pageSize>'
            '</request>'
        )
        
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
    Получает детали действия через action.get
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
            
            if analytic_id and (analytic_id == "4867" or 
                               (analytic_name and "produkty" in analytic_name.lower())):
                
                logger.info(f"Processing Produkty analytics: ID={analytic_id}, Name={analytic_name}")
                
                # Получаем данные аналитики через analitic.getData
                logger.info(f"Requesting data for analytics {analytic_id}...")
                analytic_data = get_analytics_data(analytic_id)
                if analytic_data:
                    logger.info(f"✅ Got analytics data for {analytic_id}, length: {len(analytic_data)}")
                    logger.info(f"Parsing analytics data...")
                    parsed_data = parse_analytics_data(analytic_data, task, action)
                    if parsed_data:
                        analytics_data.extend(parsed_data)
                        logger.info(f"✅ Successfully parsed {len(parsed_data)} records from analytics {analytic_id}")
                    else:
                        logger.warning(f"❌ No data parsed from analytics {analytic_id}")
                        logger.warning(f"This means the XML structure is different than expected")
                else:
                    logger.warning(f"❌ Failed to get data for analytics {analytic_id}")
                    logger.warning(f"This means analitic.getData returned empty or error response")
        
        return analytics_data
        
    except Exception as e:
        logger.error(f"Error extracting analytics data from action: {e}")
        return []

def get_analytics_data(analytic_id):
    """
    Получает данные аналитики через analitic.getData
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
            f'  <key>{analytic_id}</key>'
            '</analiticKeys>'
            '</request>'
        )
        
        logger.info(f"Requesting analytics data for ID {analytic_id}...")
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
        logger.error(f"Error getting analytics data for {analytic_id}: {e}")
        return None

def parse_analytics_data(xml_text, task, action):
    """
    Парсит данные аналитики из XML ответа analitic.getData
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
            
            # Создаем запись для Supabase
            record = {
                'task_id': task['id'],
                'task_name': task.get('name', ''),
                'action_id': action.get('id'),
                'action_text': action.get('text', ''),
                'action_datetime': action.get('dateTime', ''),
                'analytic_key': key,
                'nazwa': field_data.get('27719', {}).get('value', ''),
                'cena': field_data.get('27721', {}).get('value', ''),
                'waluta': field_data.get('29133', {}).get('value', ''),
                'ilosc': field_data.get('28079', {}).get('value', ''),
                'rabat_percent': field_data.get('28109', {}).get('value', ''),
                'cena_po_rabacie': field_data.get('28111', {}).get('value', ''),
                'wartosc_netto': field_data.get('28081', {}).get('value', ''),
                'prowizja_pln': field_data.get('29311', {}).get('value', ''),
                'laczna_masa_kg': field_data.get('32907', {}).get('value', ''),
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
        
        # Обрабатываем каждую задачу
        for task in tasks:
            task_id = task['id']
            logger.info(f"Processing task ID: {task_id}, Name: {task.get('name', 'Unknown')}")
            
            try:
                # Получаем список действий в задаче
                actions_xml = get_task_actions(task_id)
                actions = parse_task_actions(actions_xml)
                
                logger.info(f"Task {task_id} has {len(actions)} actions")
                
                # Ищем аналитику "Produkty" в действиях
                found_analytics_in_task = False
                for action in actions:
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
                            logger.info(f"  ❌ No Produkty analytics data found in action {action_id}")
                
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
