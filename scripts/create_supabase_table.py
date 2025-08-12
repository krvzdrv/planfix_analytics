#!/usr/bin/env python3
"""
Скрипт для создания таблицы в Supabase на основе структуры аналитики из Planfix.
Используется после получения структуры аналитики через Postman.
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

def get_analytics_structure(analytic_key):
    """
    Получает структуру аналитики из Planfix для определения полей таблицы
    """
    try:
        # Используем существующую функцию из planfix_utils
        params = {
            'analiticKeys': {
                'key': analytic_key
            }
        }
        
        response_xml = planfix_utils.make_planfix_request('analitic.getData', params)
        return response_xml
        
    except Exception as e:
        logger.error(f"Error getting analytics structure for key {analytic_key}: {e}")
        raise

def parse_analytics_structure(xml_text):
    """
    Парсит XML ответ от API analitic.getData для определения структуры полей
    """
    try:
        root = ET.fromstring(xml_text)
        if root.attrib.get("status") == "error":
            code = root.findtext("code")
            message = root.findtext("message")
            logger.error(f"Planfix API error: code={code}, message={message}")
            return []
        
        fields_structure = {}
        
        # Парсим analiticDatas
        for analitic_data in root.findall('.//analiticData'):
            key = analitic_data.findtext('key')
            if key is None:
                continue
                
            # Парсим itemData для определения структуры полей
            for item_data in analitic_data.findall('.//itemData'):
                field_name = item_data.findtext('name')
                if field_name:
                    # Определяем тип поля на основе значения
                    value = item_data.findtext('value')
                    value_id = item_data.findtext('valueId')
                    
                    # Анализируем тип данных
                    field_type = determine_field_type(value, value_id)
                    
                    if field_name not in fields_structure:
                        fields_structure[field_name] = {
                            'type': field_type,
                            'examples': [],
                            'has_value_id': False
                        }
                    
                    # Добавляем примеры значений
                    if value and value not in fields_structure[field_name]['examples']:
                        fields_structure[field_name]['examples'].append(value)
                    
                    if value_id:
                        fields_structure[field_name]['has_value_id'] = True
        
        return fields_structure
        
    except ET.ParseError as e:
        logger.error(f"XML ParseError: {e}")
        raise
    except Exception as e:
        logger.error(f"Error parsing analytics structure: {e}")
        raise

def determine_field_type(value, value_id):
    """
    Определяет тип поля на основе значения
    """
    if not value:
        return 'TEXT'
    
    # Проверяем, является ли значение числом
    try:
        float(value)
        if '.' in value:
            return 'NUMERIC'
        else:
            return 'INTEGER'
    except ValueError:
        pass
    
    # Проверяем, является ли значение датой
    if is_date_string(value):
        return 'TIMESTAMP'
    
    # Проверяем, является ли значение булевым
    if value.lower() in ['true', 'false', 'да', 'нет', '1', '0']:
        return 'BOOLEAN'
    
    # По умолчанию TEXT
    return 'TEXT'

def is_date_string(value):
    """
    Проверяет, является ли строка датой
    """
    date_formats = [
        '%d-%m-%Y %H:%M',
        '%d-%m-%Y',
        '%Y-%m-%d',
        '%d.%m.%Y',
        '%d/%m/%Y'
    ]
    
    for fmt in date_formats:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    
    return False

def generate_table_sql(table_name, fields_structure, analytic_key):
    """
    Генерирует SQL для создания таблицы на основе структуры аналитики
    """
    # Базовые колонки
    columns = [
        f'"{table_name}_id" TEXT PRIMARY KEY',
        f'"analitic_key" INTEGER NOT NULL',
        f'"item_id" INTEGER',
        f'"updated_at" TIMESTAMP DEFAULT NOW()',
        f'"is_deleted" BOOLEAN DEFAULT FALSE'
    ]
    
    # Добавляем колонки на основе структуры аналитики
    for field_name, field_info in fields_structure.items():
        # Очищаем название поля для SQL
        clean_field_name = field_name.replace(' ', '_').replace('-', '_').replace('.', '_')
        clean_field_name = ''.join(c for c in clean_field_name if c.isalnum() or c == '_')
        
        # Добавляем колонку
        columns.append(f'"{clean_field_name}" {field_info["type"]}')
        
        # Если есть value_id, добавляем отдельную колонку
        if field_info['has_value_id']:
            columns.append(f'"{clean_field_name}_id" TEXT')
    
    # Создаем SQL
    sql = f'''CREATE TABLE IF NOT EXISTS "{table_name}" (
    {',\n    '.join(columns)}
);

-- Создаем индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_{table_name}_analitic_key ON "{table_name}" (analitic_key);
CREATE INDEX IF NOT EXISTS idx_{table_name}_item_id ON "{table_name}" (item_id);
CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON "{table_name}" (updated_at);
CREATE INDEX IF NOT EXISTS idx_{table_name}_is_deleted ON "{table_name}" (is_deleted);

-- Добавляем комментарии к таблице
COMMENT ON TABLE "{table_name}" IS 'Данные аналитики Planfix с ключом {analytic_key}';
'''
    
    return sql

def create_table_in_supabase(table_name, sql):
    """
    Создает таблицу в Supabase
    """
    try:
        conn = planfix_utils.get_supabase_connection()
        
        with conn.cursor() as cur:
            logger.info(f"Creating table {table_name}...")
            cur.execute(sql)
            conn.commit()
            logger.info(f"Table {table_name} created successfully")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        raise

def main():
    """
    Главная функция для создания таблицы в Supabase
    """
    logger.info("--- Starting Supabase table creation ---")
    
    # Проверяем обязательные переменные окружения
    planfix_utils.check_required_env_vars({
        'PLANFIX_API_KEY': planfix_utils.PLANFIX_API_KEY,
        'PLANFIX_TOKEN': planfix_utils.PLANFIX_TOKEN,
        'PLANFIX_ACCOUNT': planfix_utils.PLANFIX_ACCOUNT,
    })

    try:
        # Запрашиваем у пользователя ключ аналитики
        analytic_key = input("Введите ключ аналитики из Planfix: ").strip()
        if not analytic_key:
            logger.error("Analytic key is required")
            return
        
        # Запрашиваем название таблицы
        table_name = input("Введите название таблицы для Supabase (по умолчанию: planfix_analytics_produkty): ").strip()
        if not table_name:
            table_name = "planfix_analytics_produkty"
        
        logger.info(f"Getting analytics structure for key: {analytic_key}")
        
        # Получаем структуру аналитики
        xml_response = get_analytics_structure(analytic_key)
        fields_structure = parse_analytics_structure(xml_response)
        
        if not fields_structure:
            logger.warning("No fields found in analytics structure")
            return
        
        logger.info(f"Found {len(fields_structure)} fields in analytics structure")
        
        # Выводим структуру полей
        print("\n=== Структура полей аналитики ===")
        for field_name, field_info in fields_structure.items():
            print(f"Поле: {field_name}")
            print(f"  Тип: {field_info['type']}")
            print(f"  Примеры: {field_info['examples'][:3]}")  # Показываем первые 3 примера
            print(f"  Есть value_id: {field_info['has_value_id']}")
            print()
        
        # Генерируем SQL для создания таблицы
        sql = generate_table_sql(table_name, fields_structure, analytic_key)
        
        # Выводим SQL
        print("=== SQL для создания таблицы ===")
        print(sql)
        
        # Спрашиваем пользователя о создании таблицы
        create_table = input("\nСоздать таблицу в Supabase? (y/n): ").strip().lower()
        if create_table in ['y', 'yes', 'да']:
            create_table_in_supabase(table_name, sql)
            print(f"Таблица {table_name} успешно создана в Supabase!")
        else:
            print("SQL сохранен, но таблица не создана")
        
        # Сохраняем SQL в файл
        sql_filename = f"create_table_{table_name}.sql"
        with open(sql_filename, 'w', encoding='utf-8') as f:
            f.write(sql)
        print(f"SQL сохранен в файл: {sql_filename}")
        
        logger.info("--- Supabase table creation finished successfully ---")

    except Exception as e:
        logger.critical(f"An error occurred in the main process: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    main()
