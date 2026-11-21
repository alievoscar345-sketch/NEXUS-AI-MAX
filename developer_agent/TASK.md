Реализуй следующий отдельный этап P0-A2: устранение синхронного SQLite I/O из asyncio event loop в agent_server backend.

Исходное состояние:
- Базовая ветка: gemini-hydrogen-development.
- В agent_server/app/main.py много async функций (worker/process/llm_json и FastAPI async endpoints), внутри которых напрямую выполняются with db() as c, provider_models_for_owner(), event(), claim_task(), _persist_plan() и другие синхронные SQLite операции.
- PR #2 содержит P0-A1 (закрытие SQLite соединений) и остаётся отдельным Draft PR. НЕ предполагай, что он уже слит. Не меняй базовую ветку и не включай его изменения в этот PR. Работай от актуального gemini-hydrogen-development.

Цель:
Все SQLite операции, которые могут выполняться из async event loop, должны выполняться в worker thread через asyncio.to_thread (или аналогичный безопасный механизм). Нельзя просто обернуть отдельный connect в to_thread, а затем использовать connection из другого потока. Открытие соединения, SQL, чтение результатов и закрытие должны происходить в одном sync helper, целиком вызванном через asyncio.to_thread.

Объём:
1. Прочитай полностью agent_server/app/main.py и тесты agent_server/tests/.
2. Составь карту всех SQLite call sites в async функциях и исправь их, включая:
   - worker(): claim_task;
   - lifespan(): init_db/recover_interrupted_tasks;
   - process(): все операции чтения/записи задач, шагов и событий;
   - llm_json(): provider_models_for_owner и event;
   - run_step(): DB/event operations;
   - все FastAPI async endpoints для providers/tasks/approvals/models.
3. Предпочтительный паттерн:
   - оставить sync DB/repository функции синхронными;
   - выделить короткие sync helper-функции для атомарных групп SQL-операций;
   - вызывать каждый helper из async контекста как await asyncio.to_thread(helper, args...);
   - не передавать sqlite3.Connection между потоками;
   - не держать SQLite transaction/connection открытым во время сетевого запроса, await, model call или длительной обработки.
4. FastAPI endpoints, не выполняющие await внутри тела и не нуждающиеся в async, можно перевести в обычные def: FastAPI сам запускает sync endpoints в threadpool. Не меняй HTTP контракт, auth, статусы, payloads.
5. Сохранить последовательность транзакций, BEGIN IMMEDIATE, idempotency, task ownership, lease/retry/cancel/approval semantics и порядок событий. Не менять схему БД, миграции, бизнес-логику и инструменты агента.
6. Не использовать глобальный sqlite connection, не создавать connection в одном потоке и не закрывать в другом. Не вводить дополнительный executor/async DB framework.
7. Добавь тесты, которые подтверждают:
   - sync DB helper исполняется не в event-loop thread (используй thread identity/event loop test, без sleep-зависимостей);
   - основные async DB pathways используют thread offload;
   - существующие durability, owner-scope, idempotency, cancellation/recovery и security тесты остаются зелёными.
   Если удобнее, тестируй отдельный общий helper async def run_db(fn,*args): return await asyncio.to_thread(fn,*args), но реальные async call sites также должны использовать offload, не оставляй их на прямых синхронных вызовах.
8. Добавь минимальные комментарии только там, где они проясняют thread boundary.
9. Измени только:
   - agent_server/app/main.py
   - agent_server/tests/ (только необходимые существующие тестовые файлы; предпочтительно test_task_durability.py).
10. Запусти python -m compileall -q app и python -m unittest discover -s tests -v. Проверь grep/аудит всех async функций: не должно остаться прямых SQLite операций в event loop. Проверь diff на отсутствие лишних изменений.
11. Создай изолированную ветку nexus-agent/<run-id> от gemini-hydrogen-development и один Draft PR в gemini-hydrogen-development. Не делай merge.
12. Если есть неоднозначность при записи/создании PR — остановись без повторных попыток и сообщи точную ошибку.

В итоговом сообщении: список изменённых файлов, схема thread-offload, перечень охваченных call sites, результаты тестов и ссылка на Draft PR. Не раскрывай секреты и не изменяй другие файлы.