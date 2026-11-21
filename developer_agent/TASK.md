Проведи ограниченный smoke-test инструментов записи NEXUS Developer Agent.

Цель: проверить write_file и create_pull_request на безопасном тестовом документе.

1. Используй list_files, чтобы убедиться, что developer_agent/ существует.
2. Создай ровно один новый файл developer_agent/SMOKE_TEST.md со следующим содержимым:

# NEXUS Developer Agent Smoke Test

This file verifies that the NEXUS Developer Agent can create a text file on an isolated branch and open a draft pull request.

- Test scope: write_file and create_pull_request
- Repository files outside this document must remain unchanged.
- This test document is temporary and must not be merged into the development branch.

3. Проверь, что write_file вернул успешный результат.
4. Создай один Draft Pull Request в gemini-hydrogen-development с кратким описанием теста.
5. Не изменяй никакие другие файлы.
6. Не запускай повторные попытки создания файла или PR, если инструмент вернул неоднозначный результат. В таком случае остановись и сообщи точную ошибку.
7. В конце укажи имя ветки, ссылку на созданный PR и результат каждого инструмента.

Это тест реальных GitHub write permissions. Не пытайся обойти ограничения инструментов.