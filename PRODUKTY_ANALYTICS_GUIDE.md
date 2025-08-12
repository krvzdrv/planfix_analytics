# Пошаговое руководство: Получение структуры аналитики "Produkty"

## Цель
Получить структуру данных аналитики "Produkty" из Planfix CRM для создания соответствующей таблицы в Supabase.

## Шаг 1: Подготовка Postman

### 1.1. Импорт коллекции
1. Откройте Postman
2. Нажмите "Import" → "File"
3. Выберите файл `Planfix_API_Postman_Collection.json`
4. Коллекция "Planfix API - Analytics" будет импортирована

### 1.2. Настройка переменных
1. В коллекции нажмите на вкладку "Variables"
2. Заполните обязательные переменные:
   - `PLANFIX_ACCOUNT` - название вашего аккаунта в Planfix
   - `PLANFIX_API_KEY` - ваш API ключ
   - `PLANFIX_TOKEN` - ваш пользовательский токен

## Шаг 2: Поиск аналитики "Produkty"

### 2.1. Поиск по названию
1. Откройте запрос **"Search Actions by Name"**
2. Убедитесь, что в переменной `SEARCH_TEXT` установлено значение "Produkty"
3. Нажмите "Send"
4. В ответе найдите действия, связанные с "Produkty"

### 2.2. Анализ ответа
В ответе вы получите XML с такой структурой:
```xml
<response status="ok">
  <actions>
    <action>
      <id>12345</id>
      <name>Название действия с Produkty</name>
      <description>Описание действия</description>
    </action>
    <!-- Другие действия -->
  </actions>
</response>
```

**Важно:** Запишите ID найденного действия!

## Шаг 3: Получение деталей действия

### 3.1. Запрос деталей
1. Откройте запрос **"Get Action Details"**
2. В переменной `ACTION_ID` установите ID, найденный в предыдущем шаге
3. Нажмите "Send"

### 3.2. Анализ аналитик
В ответе найдите секцию `<analitic>`:
```xml
<response status="ok">
  <action>
    <id>12345</id>
    <name>Название действия</name>
    <analitic>
      <id>67890</id>
      <name>Produkty</name>
      <description>Описание аналитики</description>
    </analitic>
    <!-- Другие аналитики -->
  </action>
</response>
```

**Важно:** Запишите ID аналитики "Produkty"!

## Шаг 4: Получение структуры данных аналитики "Produkty"

### 4.1. Запрос структуры
1. Откройте запрос **"Get Produkty Analytics Structure"**
2. В переменной `PRODUKTY_ANALYTIC_KEY` установите ID аналитики из предыдущего шага
3. Нажмите "Send"

### 4.2. Анализ структуры данных
В ответе вы получите XML с полной структурой данных:
```xml
<response status="ok">
  <analiticDatas>
    <analiticData>
      <key>67890</key>
      <itemData>
        <id>1</id>
        <name>Название продукта</name>
        <value>Продукт А</value>
        <valueId>prod_a</valueId>
      </itemData>
      <itemData>
        <id>2</id>
        <name>Количество</name>
        <value>100</value>
        <valueId>100</valueId>
      </itemData>
      <itemData>
        <id>3</id>
        <name>Цена</name>
        <value>150.50</value>
        <valueId>150.50</valueId>
      </itemData>
      <!-- Другие поля -->
    </analiticData>
  </analiticDatas>
</response>
```

## Шаг 5: Анализ полей аналитики

### 5.1. Определение типов данных
Анализируйте каждое поле:

| Поле | Значение | Тип данных | Есть valueId |
|------|----------|------------|--------------|
| Название продукта | Продукт А | TEXT | Да |
| Количество | 100 | INTEGER | Да |
| Цена | 150.50 | NUMERIC | Да |
| Дата создания | 01-01-2024 | TIMESTAMP | Нет |

### 5.2. Запись структуры
Создайте таблицу с полями:
```sql
-- Пример структуры таблицы
CREATE TABLE planfix_analytics_produkty (
    planfix_analytics_produkty_id TEXT PRIMARY KEY,
    analitic_key INTEGER NOT NULL,
    item_id INTEGER,
    nazvanie_produkta TEXT,
    nazvanie_produkta_id TEXT,
    kolichestvo INTEGER,
    kolichestvo_id INTEGER,
    tsena NUMERIC,
    tsena_id NUMERIC,
    data_sozdaniya TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    is_deleted BOOLEAN DEFAULT FALSE
);
```

## Шаг 6: Создание таблицы в Supabase

### 6.1. Запуск скрипта создания
```bash
python scripts/create_supabase_table.py
```

### 6.2. Ввод параметров
- Ключ аналитики: [ID аналитики "Produkty"]
- Название таблицы: `planfix_analytics_produkty`

## Шаг 7: Экспорт данных

### 7.1. Запуск скрипта экспорта
```bash
python scripts/export_produkty_analytics.py
```

### 7.2. Ввод параметров
- Ключ аналитики: [ID аналитики "Produkty"]

## Примеры поиска по ключевым словам

### В Planfix ищите:
- "Produkty"
- "Продукты"
- "Товары"
- "Аналитика продуктов"
- "Отчет по продуктам"

### Возможные названия действий:
- "Аналитика продаж по продуктам"
- "Отчет по товарам"
- "Статистика продуктов"
- "Анализ ассортимента"

## Устранение неполадок

### Ошибка "No fields found":
- Проверьте правильность ID аналитики
- Убедитесь, что аналитика содержит данные

### Пустой ответ:
- Проверьте права доступа к аналитике
- Убедитесь, что аналитика активна

### Ошибка аутентификации:
- Проверьте API ключ и токен
- Убедитесь, что токен не истек

## Следующие шаги

После получения структуры:
1. ✅ Создайте таблицу в Supabase
2. ✅ Настройте автоматический экспорт
3. ✅ Создайте дашборды для анализа данных
4. ✅ Настройте уведомления об изменениях

## Полезные команды

### Проверка подключения к Planfix:
```bash
python scripts/planfix_get_analytics_list.py
```

### Создание таблицы:
```bash
python scripts/create_supabase_table.py
```

### Экспорт данных:
```bash
python scripts/export_produkty_analytics.py
```

---

**Примечание:** Сохраните все ответы Postman для дальнейшего анализа и создания документации по структуре данных.
