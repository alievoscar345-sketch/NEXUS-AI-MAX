Реализуй первый ограниченный P0-A патч для SQLite connection lifecycle в основном NEXUS backend.

Контекст:
- Целевая ветка для PR: gemini-hydrogen-development.
- Файлы backend находятся в agent_server/.
- В agent_server/app/main.py функция db() вызывает sqlite3.connect(), а большинство мест используют конструкцию with db() as c.
- В стандартном sqlite3 контекстный менеджер Connection завершает транзакцию, но сам по себе НЕ закрывает соединение. Это приводит к утечкам файловых дескрипторов/соединений при повторных вызовах.

Задача:
1. Прочитай agent_server/app/main.py и agent_server/tests/test_task_durability.py.
2. В main.py реализуй минимальный безопасный механизм закрытия SQLite connection после выхода из with db() as c. Предпочтительно определить подкласс sqlite3.Connection с переопределённым __exit__: вызвать super().__exit__(...), а затем гарантированно вызвать close() в finally; передать этот класс в sqlite3.connect через factory=. Сохрани существующий интерфейс db() и поведение commit/rollback.
3. Добавь тесты в agent_server/tests/test_task_durability.py, которые доказывают:
   - успешный выход из with db() as c коммитит транзакцию и соединение после выхода закрыто;
   - выход с исключением откатывает транзакцию и соединение после выхода закрыто.
   Используй временную тестовую БД из существующего setUp.
4. Не выполняй масштабный рефакторинг async/event-loop в этом патче. Не меняй бизнес-логику worker, schema, миграции, API, маршруты, Android-код или другие файлы. Следующим отдельным этапом будет offloading синхронного SQLite I/O из async event loop.
5. Добавь/обнови только эти два файла:
   - agent_server/app/main.py
   - agent_server/tests/test_task_durability.py
6. Проверь diff на отсутствие посторонних изменений. Не записывай API-ключи, секреты или пользовательские данные.
7. Создай изолированную ветку nexus-agent/<run-id> от gemini-hydrogen-development и один Draft PR в gemini-hydrogen-development. Не делай merge.
8. Если есть неоднозначность при записи/создании PR — остановись без повторных попыток и сообщи точную ошибку.

В итоговом сообщении укажи изменённые файлы, суть решения, тесты и ссылку на Draft PR.