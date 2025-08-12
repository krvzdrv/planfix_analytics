# 🧪 Быстрое тестирование GitHub Action

## 🚀 Запуск теста за 2 минуты

### 1. Проверьте secrets в GitHub
```
Settings → Secrets and variables → Actions
```
Должны быть:
- `PLANFIX_API_KEY`
- `PLANFIX_TOKEN` 
- `PLANFIX_ACCOUNT`
- `SUPABASE_HOST`
- `SUPABASE_DB`
- `SUPABASE_USER`
- `SUPABASE_PASSWORD`
- `SUPABASE_PORT`

### 2. Запустите Action вручную
```
Actions → Produkty Analytics Sync → Run workflow → Run workflow
```

### 3. Следите за выполнением
- ✅ **Test Planfix connection** - подключение к API
- ✅ **Test Supabase connection** - подключение к БД  
- ✅ **Sync Produkty analytics with orders** - синхронизация данных

## 🎯 Ожидаемый результат

```
✅ Produkty analytics sync completed successfully!
Data synchronized from Planfix to Supabase

=== Статистика экспорта ===
Обработано задач: X
Экспортировано записей: Y
Таблица: planfix_analytics_produkty
```

## ⚠️ Если что-то пошло не так

### Ошибка Planfix:
- Проверьте API ключ и токен
- Убедитесь, что аккаунт правильный

### Ошибка Supabase:
- Проверьте параметры подключения
- Убедитесь, что таблица создана

### Ошибка синхронизации:
- Посмотрите логи в последнем step
- Проверьте права доступа к аналитике

## 🔄 Автоматический запуск

После успешного теста Action будет работать:
- **Каждые 6 часов** - автоматически
- **Ежедневно в 2:00 UTC** - автоматически

---

**Тест готов!** Запускайте и проверяйте работу синхронизации! 🎉
