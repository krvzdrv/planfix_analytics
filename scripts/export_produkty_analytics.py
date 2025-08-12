#!/usr/bin/env python3
"""
Скрипт для экспорта данных аналитики "x Produkty" из Planfix в Supabase.
Используется после создания таблицы через create_supabase_table.py
"""

import os
import sys
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.planfix_utils as planfix_utils

logger = logging.getLogger(__name__)

# Конфигурация для аналитики "x Produkty"
PRODUKTY_ANALYTIC_KEY = None  # Будет установлен пользователем
PRODUKTY_TABLE_NAME = "planfix_analytics_produkty"  # Название таблицы в Supabase

def get_produkty_analytics_data(analytic_key):
    """
    Получает данные аналитики "x Produkty" из Planfix
    """
    try:
        params = {
            'analiticKeys': {
                'key': analytic_key
            }
        }
        
        logger.info(f"Fetching analytics data for key: {analytic_key}")
        response_xml = planfix_utils.make_planfix_request('analitic.getData', params)
        return response_xml
        
    except Exception as e:
        logger.error(f"Error getting analytics data for key {analytic_key}: {e}")
        raise

def parse_produkty_analytics_data(xml_text, table_name):
    """
    Парсит XML ответ от API analitic.getData для аналитики "x Produkty"
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
                record_id = f"{key}_{item_id}" if item_id else f"{key}_{len(analytics_data)}"
                
                # Создаем базовую структуру записи
                record = {
                    f"{table_name}_id": record_id,
                    "analitic_key": int(key) if key else None,
                    "item_id": int(item_id) if item_id else None,
                    "updated_at": datetime.now(),
                    "is_deleted": False
                }
                
                # Добавляем поле name
                if name:
                    clean_name = name.replace(' ', '_').replace('-', '_').replace('.', '_')
                    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
                    record[clean_name] = value
                    
                    # Если есть value_id, добавляем отдельную колонку
                    if value_id:
                        record[f"{clean_name}_id"] = value_id
                
                analytics_data.append(record)
        
        return analytics_data
        
    except ET.ParseError as e:
        logger.error(f"XML ParseError: {e}")
        raise
    except Exception as e:
        logger.error(f"Error parsing analytics data: {e}")
        raise

def get_table_columns(conn, table_name):
    """
    Получает список колонок таблицы из Supabase
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}" LIMIT 0')
            columns = [desc[0] for desc in cur.description]
            return columns
    except Exception as e:
        logger.error(f"Error getting table columns: {e}")
        raise

def prepare_data_for_upsert(data_list, columns):
    """
    Подготавливает данные для upsert в Supabase
    """
    prepared_data = []
    
    for record in data_list:
        # Создаем запись только с существующими колонками
        prepared_record = {}
        for col in columns:
            if col in record:
                prepared_record[col] = record[col]
            else:
                # Устанавливаем значения по умолчанию для отсутствующих колонок
                if col == 'is_deleted':
                    prepared_record[col] = False
                elif col == 'updated_at':
                    prepared_record[col] = datetime.now()
                else:
                    prepared_record[col] = None
        
        prepared_data.append(prepared_record)
    
    return prepared_data

def export_produkty_analytics():
    """
    Главная функция экспорта аналитики "x Produkty"
    """
    logger.info("--- Starting Produkty Analytics Export ---")
    
    # Проверяем обязательные переменные окружения
    planfix_utils.check_required_env_vars({
        'PLANFIX_API_KEY': planfix_utils.PLANFIX_API_KEY,
        'PLANFIX_TOKEN': planfix_utils.PLANFIX_TOKEN,
        'PLANFIX_ACCOUNT': planfix_utils.PLANFIX_ACCOUNT,
    })

    conn = None
    try:
        # Запрашиваем у пользователя ключ аналитики
        global PRODUKTY_ANALYTIC_KEY
        if not PRODUKTY_ANALYTIC_KEY:
            PRODUKTY_ANALYTIC_KEY = input("Введите ключ аналитики 'x Produkty' из Planfix: ").strip()
            if not PRODUKTY_ANALYTIC_KEY:
                logger.error("Analytic key is required")
                return
        
        logger.info(f"Exporting analytics data for key: {PRODUKTY_ANALYTIC_KEY}")
        
        # Получаем данные аналитики из Planfix
        xml_text = get_produkty_analytics_data(PRODUKTY_ANALYTIC_KEY)
        analytics_data = parse_produkty_analytics_data(xml_text, PRODUKTY_TABLE_NAME)
        
        if not analytics_data:
            logger.info("No analytics data found")
            return
        
        logger.info(f"Total analytics records processed: {len(analytics_data)}")
        
        # Подключаемся к Supabase
        conn = planfix_utils.get_supabase_connection()
        
        # Получаем структуру таблицы
        table_columns = get_table_columns(conn, PRODUKTY_TABLE_NAME)
        logger.info(f"Table columns: {table_columns}")
        
        # Подготавливаем данные для upsert
        prepared_data = prepare_data_for_upsert(analytics_data, table_columns)
        
        # Обновляем данные в Supabase
        planfix_utils.upsert_data_to_supabase(
            conn,
            PRODUKTY_TABLE_NAME,
            f"{PRODUKTY_TABLE_NAME}_id",
            table_columns,
            prepared_data
        )
        
        # Получаем список всех ID для пометки удаленных записей
        all_ids = [item[f"{PRODUKTY_TABLE_NAME}_id"] for item in prepared_data if item.get(f"{PRODUKTY_TABLE_NAME}_id")]
        
        # Помечаем записи как удаленные
        planfix_utils.mark_items_as_deleted_in_supabase(
            conn,
            PRODUKTY_TABLE_NAME,
            f"{PRODUKTY_TABLE_NAME}_id",
            all_ids
        )
        
        logger.info("--- Produkty Analytics Export finished successfully ---")
        
        # Выводим статистику
        print(f"\n=== Статистика экспорта ===")
        print(f"Обработано записей: {len(analytics_data)}")
        print(f"Таблица: {PRODUKTY_TABLE_NAME}")
        print(f"Ключ аналитики: {PRODUKTY_ANALYTIC_KEY}")
        print(f"Время экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
        export_produkty_analytics()
    except KeyboardInterrupt:
        print("\nЭкспорт прерван пользователем")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
