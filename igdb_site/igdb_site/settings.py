"""
Django settings for igdb_site project.
"""

from pathlib import Path
import os
from dotenv import load_dotenv
import sys
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

# Определяем окружение через переменную окружения RENDER
IS_RENDER = os.getenv('RENDER', False)

SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'dev-secret-key-change-in-production')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv('DJANGO_DEBUG', 'False') == 'True'

# Оптимизации производительности
DISABLE_AUTO_CACHE_UPDATES = DEBUG  # True в DEBUG, False в production
CACHE_UPDATE_BATCH_SIZE = 100  # Размер батча для массовых обновлений
CACHE_UPDATE_TIMEOUT_HOURS = 24  # Часы между автообновлениями

# Настройка разрешённых хостов
if DEBUG:
    ALLOWED_HOSTS = ['*']
else:
    allowed_hosts_str = os.getenv('ALLOWED_HOSTS', 'localhost')
    ALLOWED_HOSTS = [host.strip() for host in allowed_hosts_str.split(',')]

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
    'whitenoise.middleware.WhiteNoiseMiddleware',
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
                'games.context_processors.debug_context',
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

import os
import dj_database_url

IS_RAILWAY = os.getenv('RAILWAY') == 'true'
IS_DESKTOP = os.getenv('DESKTOP_MODE') == '1'

if IS_RAILWAY:
    # Берём DATABASE_URL только из переменной окружения
    database_url = os.getenv('DATABASE_URL')

    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set in Railway!")

    DATABASES = {
        'default': dj_database_url.config(
            default=database_url,
            conn_max_age=600,
            conn_health_checks=True,
            ssl_require=False
        )
    }
    print("[RAILWAY] PostgreSQL configured via DATABASE_URL from environment")

elif IS_DESKTOP:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.getenv('DB_NAME'),
            'USER': os.getenv('DB_USER'),
            'PASSWORD': os.getenv('DB_PASSWORD'),
            'HOST': os.getenv('DB_HOST'),
            'PORT': os.getenv('DB_PORT'),
            'CONN_MAX_AGE': 600,
            'OPTIONS': {
                'connect_timeout': 10,
                'client_encoding': 'UTF8',
                'sslmode': 'disable',
            },
        }
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.getenv('DB_NAME'),
            'USER': os.getenv('DB_USER'),
            'PASSWORD': os.getenv('DB_PASSWORD'),
            'HOST': os.getenv('DB_HOST'),
            'PORT': os.getenv('DB_PORT'),
            'CONN_MAX_AGE': 600,
            'OPTIONS': {
                'connect_timeout': 10,
                'client_encoding': 'UTF8',
                'sslmode': 'prefer',
            },
        }
    }

# ============================================
# DESKTOP MODE SETTINGS (for .exe launcher)
# MUST BE BEFORE DATABASE CONNECTION CHECK
# ============================================

if os.getenv('DESKTOP_MODE') == '1':
    # Disable SSL for embedded PostgreSQL - MUST BE FIRST
    if 'default' in DATABASES:
        if 'OPTIONS' not in DATABASES['default']:
            DATABASES['default']['OPTIONS'] = {}
        DATABASES['default']['OPTIONS']['sslmode'] = 'disable'
        if 'ssl_require' in DATABASES['default']:
            DATABASES['default'].pop('ssl_require')

    # Disable debug mode for desktop
    DEBUG = True

    # Allow all hosts in desktop mode
    ALLOWED_HOSTS = ['*']

    # Remove debug_toolbar if present
    if 'debug_toolbar' in INSTALLED_APPS:
        INSTALLED_APPS = [app for app in INSTALLED_APPS if app != 'debug_toolbar']

    # Remove debug_toolbar from middleware if present
    MIDDLEWARE = [m for m in MIDDLEWARE if 'debug_toolbar' not in m]

    # Disable debug toolbar config
    DEBUG_TOOLBAR_CONFIG = None

    # Clear internal IPs for debug toolbar
    INTERNAL_IPS = []

    # Disable timezone for embedded PostgreSQL
    USE_TZ = False
    TIME_ZONE = 'Europe/Moscow'

    # Disable PostgreSQL extensions that pgembed doesn't support
    DATABASE_AUTO_OPTIMIZE = False
    CREATE_EXTENDED_INDEXES = False
    USE_POSTGRES_TRGM = False
    USE_POSTGRES_GIN = False

    # ============================================
    # СТАТИЧЕСКИЕ ФАЙЛЫ ДЛЯ DESKTOP РЕЖИМА
    # ============================================
    if getattr(sys, 'frozen', False):
        base_dir = Path(sys.executable).parent
    else:
        base_dir = Path(__file__).resolve().parent.parent

    STATIC_URL = '/static/'
    STATIC_ROOT = base_dir / 'staticfiles'

    # В desktop-режиме STATICFILES_DIRS должен быть пустым
    STATICFILES_DIRS = []

    print(f"[DESKTOP MODE] STATIC_ROOT: {STATIC_ROOT}")
    print(f"[DESKTOP MODE] STATIC_URL: {STATIC_URL}")
    print(f"[DESKTOP MODE] DEBUG: {DEBUG}")

    # ============================================
    # ЛОГИРОВАНИЕ
    # ============================================
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '{levelname} {message}',
                'style': '{',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
            },
        },
        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },
        'loggers': {
            'django': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': False,
            },
            'django.template': {
                'handlers': ['console'],
                'level': 'WARNING',
                'propagate': False,
            },
            'games': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': False,
            },
        },
    }

    # ============================================
    # КЭШ
    # ============================================
    CACHES = {
        'default': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'desktop-cache',
        },
        'page_cache': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'page-cache',
        },
        'template_cache': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'template-cache',
        },
    }

    print("[DESKTOP MODE] Settings applied")

# ============================================
# АВТОМАТИЧЕСКИЕ НАСТРОЙКИ ДЛЯ HUGGING FACE
# ============================================

# Определяем, что мы на Hugging Face Spaces
IS_HF_SPACE = bool(os.getenv('SPACE_AUTHOR'))

if IS_HF_SPACE:
    # Отключаем DEBUG режим на продакшне
    DEBUG = False

    # Разрешаем только домены Hugging Face и localhost
    ALLOWED_HOSTS = ['.hf.space', 'localhost', '127.0.0.1']

    # Настройка для работы за прокси Hugging Face
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

    # Безопасные cookie
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True

    # Отключаем debug toolbar на продакшне
    MIDDLEWARE = [m for m in MIDDLEWARE if m != 'debug_toolbar.middleware.DebugToolbarMiddleware']
    INSTALLED_APPS = [app for app in INSTALLED_APPS if app != 'debug_toolbar']

    # Используем локальный кэш вместо файлового (файловая система на Hugging Face временная)
    CACHES = {
        'default': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'unique-snowflake',
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
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Поддержка ES6 модулей
SECURE_CROSS_ORIGIN_OPENER_POLICY = None

# Media files
MEDIA_URL = 'media/'
MEDIA_ROOT = BASE_DIR / 'media'

# WhiteNoise для раздачи статики в production
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

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
STEAM_API_KEY = os.getenv('STEAM_API_KEY')

# Pollinations.AI API Configuration
POLLINATIONS_API_KEY = os.getenv('POLLINATIONS_API_KEY')
POLLINATIONS_DEFAULT_MODEL = 'openai'
POLLINATIONS_TIMEOUT = 30

# ============================================
# КЭШИРОВАНИЕ ДЛЯ УСКОРЕНИЯ
# ============================================

if not os.getenv('DESKTOP_MODE') == '1':
    CACHES = {
        'default': {
            'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
            'LOCATION': os.path.join(BASE_DIR, 'django_cache'),
            'TIMEOUT': 3600,
            'OPTIONS': {
                'MAX_ENTRIES': 1000,
            }
        },
        'page_cache': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'page-cache',
            'TIMEOUT': 900,
        },
        'template_cache': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'template-cache',
            'TIMEOUT': 3600,
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
    'debug_toolbar.panels.sql.SQLPanel',
    'debug_toolbar.panels.staticfiles.StaticFilesPanel',
    'debug_toolbar.panels.templates.TemplatesPanel',
    'debug_toolbar.panels.cache.CachePanel',
    'debug_toolbar.panels.signals.SignalsPanel',
]

DEBUG_TOOLBAR_CONFIG = {
    'SHOW_COLLAPSED': True,
    'SHOW_TOOLBAR_CALLBACK': lambda request: DEBUG,
    'RESULTS_CACHE_SIZE': 100,
    'SQL_WARNING_THRESHOLD': 500,
    'ENABLE_STACKTRACES': True,
    'SHOW_TEMPLATE_CONTEXT': True,
    'DISABLE_PANELS': {
        'debug_toolbar.panels.redirects.RedirectsPanel',
    },
    'PRINT_SQL': False,
    'SQL_EXPLAIN': True,
    'SQL_PARAMS': True,
    'PROFILER_MAX_DEPTH': 10,
}

# ============================================
# ЛОГИРОВАНИЕ С ОПТИМИЗАЦИЕЙ
# ============================================

if not os.getenv('DESKTOP_MODE') == '1':
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
                'level': 'WARNING',
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

# ============================================
# НАСТРОЙКИ ОПТИМИЗАЦИИ БАЗЫ ДАННЫХ (POSTGRESQL)
# ============================================

# Автоматически оптимизировать базу данных при первом запросе в DEBUG режиме
DATABASE_AUTO_OPTIMIZE = False

# Создавать дополнительные индексы
CREATE_EXTENDED_INDEXES = True

# PostgreSQL специфичные настройки
POSTGRESQL_OPTIMIZATIONS = {
    'ENABLE_SEQSCAN': True,
    'ENABLE_HASHJOIN': True,
    'WORK_MEM': '64MB',
    'MAINTENANCE_WORK_MEM': '512MB',
}

# Настройки для PostgreSQL расширений
USE_POSTGRES_TRGM = True
USE_POSTGRES_GIN = True

# ============================================
# НАСТРОЙКИ ПРИЛОЖЕНИЯ GAMES
# ============================================

# Количество игр на странице
GAMES_PER_PAGE = 20

# Порог сходства для похожих игр
SIMILARITY_THRESHOLD = 0.15

# Время кэширования в секундах
CACHE_TIMES = {
    'similar_games': 86400,
    'filtered_games': 1800,
    'full_page': 300,
    'filter_data': 3600,
}

# ============================================
# ФИНАЛЬНЫЕ СООБЩЕНИЯ ПРИ ЗАПУСКЕ
# ============================================

# Проверка подключения к PostgreSQL
try:
    from django.db import connections

    conn = connections['default']
    conn.ensure_connection()

    db_info = f"""
[OK] Django settings loaded
[INFO] Mode: {'DEBUG' if DEBUG else 'PRODUCTION'}
[INFO] Platform: {'Railway' if IS_RAILWAY else ('Desktop' if IS_DESKTOP else 'Local')}
[INFO] Database: PostgreSQL
[INFO] Cache: FileBasedCache
[INFO] Debug Toolbar: {'ON' if DEBUG else 'OFF'}
[INFO] PostgreSQL connection: SUCCESS
"""
except Exception as e:
    db_info = f"""
[ERROR] PostgreSQL connection error: {e}
[WARNING] Check:
  1. Is PostgreSQL service running?
  2. Are .env settings correct?
"""
    if not DEBUG:
        raise
    print(db_info)

print(db_info)

# Проверка обязательных настроек
required_settings = ['IGDB_CLIENT_ID', 'IGDB_CLIENT_SECRET']
for setting in required_settings:
    if not globals().get(setting):
        print(f"[WARNING] {setting} is not set")
