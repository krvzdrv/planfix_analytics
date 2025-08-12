# 🚀 Быстрый старт: Аналитика "Produkty"

## Что нужно сделать за 5 минут

### 1. Импортируйте коллекцию Postman
- Откройте Postman
- Import → File → `Planfix_API_Postman_Collection.json`

### 2. Настройте переменные
```
PLANFIX_ACCOUNT = ваш_аккаунт
PLANFIX_API_KEY = ваш_api_ключ  
PLANFIX_TOKEN = ваш_токен
```

### 3. Выполните 3 запроса по порядку

#### Запрос 1: Поиск "Produkty"
- Откройте **"Search Actions by Name"**
- `SEARCH_TEXT = "Produkty"`
- Нажмите Send
- **Запишите ID действия**

#### Запрос 2: Детали действия
- Откройте **"Get Action Details"**
- `ACTION_ID = [ID из шага 1]`
- Нажмите Send
- **Запишите ID аналитики "Produkty"**

#### Запрос 3: Структура данных
- Откройте **"Get Produkty Analytics Structure"**
- `PRODUKTY_ANALYTIC_KEY = [ID аналитики из шага 2]`
- Нажмите Send
- **Получите полную структуру данных!**

## 🎯 Результат
В ответе на 3-й запрос вы получите XML с полями аналитики:
- Названия полей
- Типы данных
- Примеры значений
- Структуру для создания таблицы

## 📋 Следующие шаги
1. Создайте таблицу: `python scripts/create_supabase_table.py`
2. Экспортируйте данные: `python scripts/export_produkty_analytics.py`

---
**Время выполнения: 5-10 минут**
**Результат: Полная структура аналитики "Produkty"**
