"""

WSGI config for igdb_site project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see

https://docs.djangoproject.com/en/5.2/howto/deployment/wsgi/

"""

import os

from django.core.wsgi import get_wsgi_application

# Изменено: settings module указывает на правильный путь с учетом вложенности папок
# Проект имеет структуру igdb_site/igdb_site/igdb_site/, поэтому нужен полный путь
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.igdb_site.settings')

application = get_wsgi_application()