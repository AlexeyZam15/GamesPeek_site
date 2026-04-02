#!/usr/bin/env bash
# Build script for Render deployment
set -o errexit

# Устанавливаем зависимости
pip install -r requirements.txt

# Собираем статические файлы
python manage.py collectstatic --noinput

# Выполняем миграции базы данных
python manage.py migrate --noinput