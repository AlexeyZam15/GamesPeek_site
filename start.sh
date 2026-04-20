#!/bin/bash

echo "🚀 Starting Tailscale..."

# Запускаем Tailscale в фоне
tailscaled --state=/tmp/tailscale.state &
sleep 5

# Подключаемся к сети Tailscale (ключ из переменной окружения Railway)
tailscale up --auth-key=${TAILSCALE_AUTH_KEY}

echo "✅ Tailscale connected"

echo "📦 Running migrations..."

# Переходим в папку с manage.py
cd igdb_site

# Запускаем миграции
python manage.py migrate --noinput

echo "🚀 Starting Gunicorn..."

# Запускаем сервер
gunicorn igdb_site.wsgi:application