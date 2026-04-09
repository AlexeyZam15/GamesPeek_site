from django.apps import AppConfig


class DesktopMigrationsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'desktop_migrations'
    label = 'desktop_migrations'

    def ready(self):
        import desktop_migrations.migrations  # noqa