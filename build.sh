#!/usr/bin/env bash
# Build script for Render deployment
set -o errexit

# Устанавливаем зависимости
pip install -r requirements.txt

# Переходим в папку с manage.py (внутренняя igdb_site)
cd igdb_site

# Собираем статические файлы
python manage.py collectstatic --noinput

# Выполняем миграции базы данных
python manage.py migrate --noinput