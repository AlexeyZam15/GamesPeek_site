#!/bin/bash

echo "🚀 Starting Django with external PostgreSQL via zrok"

# Принудительно устанавливаем DATABASE_URL (временное решение для отладки)
export DATABASE_URL="postgresql://django_user:django_user@9hnm1kvkz1sx.zrok.io:443/gamespeek?sslmode=disable"

echo "DATABASE_URL is set to: postgresql://django_user:***@9hnm1kvkz1sx.zrok.io:443/gamespeek?sslmode=disable"

echo "📦 Running migrations..."

cd igdb_site

# Проверяем, видит ли Django переменную
python manage.py shell -c "import os; print('Django sees DATABASE_URL:', os.getenv('DATABASE_URL'))"

python manage.py migrate --noinput

echo "🚀 Starting Gunicorn..."

gunicorn igdb_site.wsgi:application