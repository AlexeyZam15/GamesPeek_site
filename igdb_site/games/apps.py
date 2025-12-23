"""
App config for games.
"""

from django.apps import AppConfig


class GamesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'games'

    def ready(self):
        """
        Инициализация приложения.
        Только подключаем сигналы без оптимизаций БД.
        """
        # Подключаем сигналы (если они нужны для других целей)
        import games.signals  # noqa