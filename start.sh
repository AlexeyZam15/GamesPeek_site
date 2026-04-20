#!/bin/bash

echo "🚀 Starting Tailscale..."

# Запускаем Tailscale в фоне
tailscaled --state=/tmp/tailscale.state &
sleep 5

# Подключаемся к твоей сети Tailscale (ключ из переменной окружения)
tailscale up --auth-key=${TAILSCALE_AUTH_KEY}

echo "✅ Tailscale connected"

# PostgreSQL доступен по IP твоего ПК в сети Tailscale
export DATABASE_URL="postgresql://django_user:django_user@100.66.92.91:5432/gamespeek?sslmode=disable"

echo "📦 Running migrations..."

# Переходим в папку с manage.py
cd igdb_site

# Запускаем миграции
python manage.py migrate --noinput

echo "🚀 Starting Gunicorn..."

# Запускаем сервер
gunicorn igdb_site.wsgi:application