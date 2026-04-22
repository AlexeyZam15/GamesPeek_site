"""
App config for games application.

Handles initialization of signals and background tasks.
"""

from django.apps import AppConfig
from django.db.models.signals import post_migrate
import logging
import random

logger = logging.getLogger(__name__)


def init_similarity_cache(sender, **kwargs):
    """
    Инициализация или прогрев кэша после миграции.
    Вызывается однократно после применения миграций.
    """
    try:
        from .models import Game
        # Проверяем, что таблица существует и имеет наши новые поля
        if hasattr(Game, 'genre_ids'):
            logger.info("Game model with materialized vectors detected")

            # Проверяем, нужно ли заполнить векторы для игр с пустыми массивами
            empty_vectors_count = Game.objects.filter(genre_ids=[]).count()
            if empty_vectors_count > 0:
                logger.warning(
                    f"Found {empty_vectors_count} games with empty vectors. Run 'populate_game_vectors' command.")
    except Exception as e:
        logger.error(f"Error during post_migrate init: {e}")


def reset_cache_version():
    """
    Сбрасывает версию кэша при рестарте сервера.
    Генерирует новую случайную версию, делая весь старый кэш недействительным.
    """
    try:
        from django.core.cache import cache

        # Генерируем новую случайную версию
        new_version = f"v{random.randint(1, 999999)}"

        # Сохраняем версию в cache
        cache.set('filter_cache_version', new_version, 86400)

        logger.info(f"Filter cache version reset to: {new_version}")
        print(f"✅ Filter cache version reset to: {new_version}")

    except Exception as e:
        logger.error(f"Failed to reset cache version: {e}")


class GamesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'games'
    verbose_name = "Game Database"

    def ready(self):
        """
        Инициализация приложения Django.
        Вызывается один раз при старте сервера.

        Важно: НЕ выполняем здесь тяжелых операций с БД,
        только подключение сигналов и легковесные проверки.
        """
        # Сбрасываем версию кэша при рестарте сервера
        reset_cache_version()

        # Импортируем сигналы для их регистрации
        try:
            import games.signals  # noqa
            logger.debug("Game signals registered successfully")
        except ImportError as e:
            logger.error(f"Failed to import game signals: {e}")
        except Exception as e:
            logger.error(f"Error registering game signals: {e}")

        # Подключаем пост-миграционный хук (выполняется после migrate)
        post_migrate.connect(init_similarity_cache, sender=self)