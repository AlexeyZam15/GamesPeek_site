"""
Django settings for igdb_site project.
"""

from pathlib import Path
import os
from dotenv import load_dotenv

import warnings

warnings.filterwarnings("ignore",
                        message="DateTimeField.*received a naive datetime.*time zone support is active")

# Загружаем переменные окружения из .env файла
load_dotenv()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# ============================================
# НАСТРОЙКИ БЕЗОПАСНОСТИ И БАЗОВЫЕ
# ============================================

SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'dev-secret-key-change-in-production')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

# Оптимизации производительности
DISABLE_AUTO_CACHE_UPDATES = DEBUG  # True в DEBUG, False в production
CACHE_UPDATE_BATCH_SIZE = 100  # Размер батча для массовых обновлений
CACHE_UPDATE_TIMEOUT_HOURS = 24  # Часы между автообновлениями

ALLOWED_HOSTS = ['*'] if DEBUG else ['yourdomain.com', 'localhost']

# ============================================
# УСКОРЕННЫЕ НАСТРОЙКИ ПРИЛОЖЕНИЙ И MIDDLEWARE
# ============================================

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'debug_toolbar',
    'games',
]

# ОПТИМИЗИРОВАННЫЙ ПОРЯДОК MIDDLEWARE
MIDDLEWARE = [
    'debug_toolbar.middleware.DebugToolbarMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'igdb_site.urls'

# ============================================
# ОПТИМИЗАЦИЯ ШАБЛОНОВ
# ============================================

# В settings.py измените контекстный процессор на относительный путь:
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'games.context_processors.debug_context',  # Исправленный путь
            ],
            'debug': DEBUG,
            'string_if_invalid': '' if not DEBUG else 'INVALID',
        },
    },
]

WSGI_APPLICATION = 'igdb_site.wsgi.application'

# ============================================
# POSTGRESQL НАСТРОЙКИ БАЗЫ ДАННЫХ
# ============================================

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('DB_NAME', 'gamespeek'),
        'USER': os.getenv('DB_USER', 'django_user'),
        'PASSWORD': os.getenv('DB_PASSWORD', 'django_user'),
        'HOST': os.getenv('DB_HOST', 'localhost'),
        'PORT': os.getenv('DB_PORT', '5432'),
        'CONN_MAX_AGE': 600,  # Долговременные соединения для скорости
        'OPTIONS': {
            'connect_timeout': 10,
            'client_encoding': 'UTF8',
            'sslmode': 'prefer',
        },
        'TEST': {
            'NAME': 'test_gamespeek',
        }
    }
}

# ============================================
# ПАРОЛИ И ВАЛИДАЦИЯ (ОПТИМИЗИРОВАННЫЕ)
# ============================================

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
        'OPTIONS': {
            'min_length': 8,
        }
    },
]

# Упрощаем валидацию для скорости в DEBUG режиме
if DEBUG:
    AUTH_PASSWORD_VALIDATORS = [
        {
            'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
        },
    ]

# ============================================
# МЕЖДУНАРОДНЫЕ НАСТРОЙКИ
# ============================================

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# ============================================
# СТАТИЧЕСКИЕ И МЕДИА ФАЙЛЫ
# ============================================

STATIC_URL = '/static/'
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'games', 'static'),
]
STATIC_ROOT = BASE_DIR / 'staticfiles'  # Для сбора статики в production

# Поддержка ES6 модулей
SECURE_CROSS_ORIGIN_OPENER_POLICY = None  # или 'same-origin'

# Media files
MEDIA_URL = 'media/'
MEDIA_ROOT = BASE_DIR / 'media'

# ============================================
# ПОЛЕ ПО УМОЛЧАНИЮ
# ============================================

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# ============================================
# API КЛЮЧИ
# ============================================

IGDB_CLIENT_ID = os.getenv('IGDB_CLIENT_ID')
IGDB_CLIENT_SECRET = os.getenv('IGDB_CLIENT_SECRET')
RAWG_API_KEY = os.getenv('RAWG_API_KEY')
STEAM_API_KEY = os.getenv('STEAM_API_KEY')  # Добавляем Steam API ключ

# Проверяем, что переменные окружения загружены
if not IGDB_CLIENT_ID or not IGDB_CLIENT_SECRET:
    raise Exception("IGDB_CLIENT_ID and IGDB_CLIENT_SECRET must be set in .env file")

# Опционально: проверка RAWG API ключа
if not RAWG_API_KEY:
    print("⚠️ Warning: RAWG_API_KEY is not set in .env file")

# Опционально: проверка Steam API ключа
if not STEAM_API_KEY:
    print("⚠️ Warning: STEAM_API_KEY is not set in .env file")

# ============================================
# КЭШИРОВАНИЕ ДЛЯ УСКОРЕНИЯ
# ============================================

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': os.path.join(BASE_DIR, 'django_cache'),  # Папка для кэша в корне проекта
        'TIMEOUT': 3600,  # 1 час для данных загрузки
        'OPTIONS': {
            'MAX_ENTRIES': 1000,  # Меньше записей, но каждая может быть большой
        }
    },
    'page_cache': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'page-cache',
        'TIMEOUT': 900,  # 15 минут для страниц
    },
    'template_cache': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'template-cache',
        'TIMEOUT': 3600,  # 1 час для шаблонов
    }
}

# Оптимизация сессий - храним в кэше
SESSION_ENGINE = 'django.contrib.sessions.backends.cached_db'
SESSION_CACHE_ALIAS = 'default'
SESSION_COOKIE_AGE = 1209600  # 2 недели

# ============================================
# DJANGO DEBUG TOOLBAR CONFIGURATION
# ============================================

# Для работы Debug Toolbar на localhost
INTERNAL_IPS = ['127.0.0.1', 'localhost']

# Оптимизированные панели Debug Toolbar
DEBUG_TOOLBAR_PANELS = [
    'debug_toolbar.panels.versions.VersionsPanel',
    'debug_toolbar.panels.timer.TimerPanel',
    'debug_toolbar.panels.settings.SettingsPanel',
    'debug_toolbar.panels.headers.HeadersPanel',
    'debug_toolbar.panels.request.RequestPanel',
    'debug_toolbar.panels.sql.SQLPanel',  # Самая важная панель для оптимизации!
    'debug_toolbar.panels.staticfiles.StaticFilesPanel',
    'debug_toolbar.panels.templates.TemplatesPanel',
    'debug_toolbar.panels.cache.CachePanel',
    'debug_toolbar.panels.signals.SignalsPanel',
]

DEBUG_TOOLBAR_CONFIG = {
    'SHOW_COLLAPSED': True,
    'SHOW_TOOLBAR_CALLBACK': lambda request: DEBUG,
    'RESULTS_CACHE_SIZE': 100,  # Количество запросов в истории
    'SQL_WARNING_THRESHOLD': 500,  # Порог для предупреждения о медленных запросах (мс)
    'ENABLE_STACKTRACES': True,
    'SHOW_TEMPLATE_CONTEXT': True,
    # Оптимизации
    'DISABLE_PANELS': {
        'debug_toolbar.panels.redirects.RedirectsPanel',
    },
    'PRINT_SQL': False,  # Отключаем для скорости
    'SQL_EXPLAIN': True,  # Включаем EXPLAIN для анализа запросов
    'SQL_PARAMS': True,
    'PROFILER_MAX_DEPTH': 10,
}

# ============================================
# ЛОГИРОВАНИЕ С ОПТИМИЗАЦИЕЙ
# ============================================

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'filters': {
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        },
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO' if DEBUG else 'WARNING',
            'filters': ['require_debug_true'],
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
        'file': {
            'level': 'ERROR',
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'django.log',
            'formatter': 'verbose',
        },
        # Обработчик для медленных SQL запросов
        'slow_sql': {
            'level': 'WARNING',
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'slow_sql.log',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
        'django.db.backends': {
            'level': 'WARNING',  # Только предупреждения и ошибки
            'handlers': ['console', 'slow_sql'],
            'propagate': False,
        },
        'games': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate': False,
        },
    },
}

# Создаем папку для логов если её нет
os.makedirs(BASE_DIR / 'logs', exist_ok=True)

# ============================================
# ОПТИМИЗАЦИИ ДЛЯ РАЗРАБОТКИ
# ============================================

if DEBUG:
    # Увеличиваем лимиты для загрузки данных
    DATA_UPLOAD_MAX_MEMORY_SIZE = 52428800  # 50MB
    FILE_UPLOAD_MAX_MEMORY_SIZE = 52428800  # 50MB

    # Отключаем некоторые проверки безопасности для скорости
    SECURE_BROWSER_XSS_FILTER = False
    SECURE_CONTENT_TYPE_NOSNIFF = False
    X_FRAME_OPTIONS = 'SAMEORIGIN'

    # Для Docker/WSL добавляем автоматическое определение IP
    try:
        import socket

        hostname, _, ips = socket.gethostbyname_ex(socket.gethostname())
        INTERNAL_IPS += [ip[:-1] + '1' for ip in ips]
    except:
        pass

    # Авто-перезагрузка шаблонов
    TEMPLATES[0]['OPTIONS']['debug'] = True

    # Быстрая загрузка статики
    STATICFILES_STORAGE = 'django.contrib.staticfiles.storage.StaticFilesStorage'

# ============================================
# НАСТРОЙКИ ОПТИМИЗАЦИИ БАЗЫ ДАННЫХ (POSTGRESQL)
# ============================================

# Автоматически оптимизировать базу данных при первом запросе в DEBUG режиме
DATABASE_AUTO_OPTIMIZE = False  # Отключаем для PostgreSQL

# Создавать дополнительные индексы
CREATE_EXTENDED_INDEXES = True

# PostgreSQL специфичные настройки
POSTGRESQL_OPTIMIZATIONS = {
    'ENABLE_SEQSCAN': True,  # Разрешить последовательное сканирование
    'ENABLE_HASHJOIN': True,
    'WORK_MEM': '64MB',  # Память для операций
    'MAINTENANCE_WORK_MEM': '512MB',  # Для создания индексов
}

# Настройки для PostgreSQL расширений
USE_POSTGRES_TRGM = True  # Использовать pg_trgm для похожести строк
USE_POSTGRES_GIN = True  # Использовать GIN индексы

# ============================================
# НАСТРОЙКИ ПРИЛОЖЕНИЯ GAMES
# ============================================

# Количество игр на странице
GAMES_PER_PAGE = 20

# Порог сходства для похожих игр
SIMILARITY_THRESHOLD = 0.15

# Время кэширования в секундах
CACHE_TIMES = {
    'similar_games': 86400,  # 24 часа
    'filtered_games': 1800,  # 30 минут
    'full_page': 300,  # 5 минут
    'filter_data': 3600,  # 1 час
}

# ============================================
# ФИНАЛЬНЫЕ СООБЩЕНИЯ ПРИ ЗАПУСКЕ
# ============================================

# Проверка подключения к PostgreSQL
import sys

try:
    from django.db import connections

    conn = connections['default']
    conn.ensure_connection()

    db_info = f"""
✅ Настройки Django загружены
📁 Режим: {'DEBUG' if DEBUG else 'PRODUCTION'}
🔧 База данных: PostgreSQL (gamespeek)
⚡ Кэширование: LocMemCache (3 уровня)
📊 Debug Toolbar: {'Включен' if DEBUG else 'Выключен'}
🔗 Подключение к PostgreSQL: УСПЕШНО
📊 Драйвер: {conn.vendor} {conn.pg_version if hasattr(conn, 'pg_version') else ''}
"""
except Exception as e:
    db_info = f"""
❌ Ошибка подключения к PostgreSQL: {e}
⚠️  Проверьте:
  1. Запущена ли служба PostgreSQL
  2. Корректны ли настройки в .env файле
  3. Существует ли база 'gamespeek' и пользователь 'django_user'
"""
    print(db_info, file=sys.stderr)
    # В режиме DEBUG можно продолжить, в production - нет
    if not DEBUG:
        raise

print(db_info)

# Проверка обязательных настроек
required_settings = ['IGDB_CLIENT_ID', 'IGDB_CLIENT_SECRET']
for setting in required_settings:
    if not globals().get(setting):
        print(f"⚠️  Внимание: {setting} не установлен")