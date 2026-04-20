#!/bin/bash

echo "🚀 Starting Django with external PostgreSQL via zrok"

echo "📦 Running migrations..."

cd igdb_site

python manage.py migrate --noinput

echo "🚀 Starting Gunicorn...."

gunicorn igdb_site.wsgi:application