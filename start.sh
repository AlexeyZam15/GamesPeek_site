#!/bin/bash

echo "🚀 Starting zrok tunnel setup..."

# Скачиваем и устанавливаем zrok на Railway
curl -L https://github.com/openziti/zrok/releases/latest/download/zrok_linux_amd64.tar.gz -o /tmp/zrok.tar.gz
tar -xzf /tmp/zrok.tar.gz -C /tmp
chmod +x /tmp/zrok

# Активируем окружение с твоим токеном
/tmp/zrok enable kSohRKLzWITk

# Подключаемся к твоему приватному туннелю
/tmp/zrok access private 2mrvu0pcpwl4 --bind 0.0.0.0:5432 &

# Ждем подключения
sleep 8

# Переходим в папку где лежит manage.py (это igdb_site/igdb_site/)
cd igdb_site

# Запускаем миграции
python manage.py migrate --noinput

# Запускаем сервер (wsgi.py лежит в igdb_site/igdb_site/igdb_site/)
gunicorn igdb_site.wsgi:application