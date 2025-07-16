# calc-tou-api

[![pipeline status](https://gitlab.pgkweb.ru/%{project_path}/badges/%{default_branch}/pipeline.svg)](https://gitlab.pgkweb.ru/%{project_path}/-/commits/%{default_branch})
[![coverage report](https://gitlab.pgkweb.ru/%{project_path}/badges/%{default_branch}/coverage.svg)](https://gitlab.pgkweb.ru/%{project_path}/-/commits/%{default_branch})

## Установка и запуск проекта

Перейти в корень проекта: /calc-tou-api/

```bash
pip install poetry==1.7.1 # Установка poetry
poetry self update --preview  # актуализируем версию poetry
poetry lock # создать poetry.lock
poetry install # Установка зависимостей из файла pyproject.toml
poetry run server # Локальный запуск uvicorn
```
```bash
если poetry lock долго отрабатывает, можно зависимости установить:
poetry export -f requirements.txt > requirements.txt
python -m pip install -r requirements.txt
```

#### [Общие конфиги по стандартам разработки](https://conf.pgk.rzd/display/DDEV/Code+Quality)

#### pre-commit hooks:

После клонирования репозитория обязательно установить pre-commit hooks.

Через poetry

```bash
poetry add -D pre-commit
poetry run pre-commit install
```

Глобальное окружение

```bash
pip install pre-commit
pre-commit install
```

#### [Commit Message Guidelines](https://conf.pgk.rzd/display/DDEV/Commit+Message+Guidelines)

#### [Git-Flow](https://conf.pgk.rzd/display/DDEV/Git-Flow)


### Migration
1. Generate migration
   ```
   alembic revision --autogenerate
   ``
2. Upgrade database
   ```
   alembic upgrade head
   ```

## Features:

TODO: Please add project description

#### [TODO: add confluence docs](https://conf.pgk.rzd/)
