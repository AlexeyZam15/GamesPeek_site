from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Game
from tqdm import tqdm


class Command(BaseCommand):
    help = 'Обновляет материализованные векторы (genre_ids, keyword_ids, engine_ids и т.д.) для всех игр'

    def handle(self, *args, **options):
        self.stdout.write('🔄 ЗАПУСК ОБНОВЛЕНИЯ МАТЕРИАЛИЗОВАННЫХ ВЕКТОРОВ')
        self.stdout.write('=' * 60)

        # Получаем все игры
        total_games = Game.objects.count()
        self.stdout.write(f'📊 Всего игр в базе: {total_games}')

        if total_games == 0:
            self.stdout.write(self.style.WARNING('⚠️  Нет игр для обновления'))
            return

        # Статистика
        updated_count = 0
        error_count = 0
        engine_updates = 0
        genre_updates = 0
        keyword_updates = 0
        theme_updates = 0
        perspective_updates = 0
        developer_updates = 0
        gamemode_updates = 0

        # Компактный прогресс-бар
        with tqdm(total=total_games, desc="Обновление", unit="игр",
                  bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}') as pbar:
            for game in Game.objects.all().iterator(chunk_size=100):
                try:
                    # Сохраняем старые значения для статистики
                    old_engine_ids = len(game.engine_ids) if game.engine_ids else 0
                    old_genre_ids = len(game.genre_ids) if game.genre_ids else 0
                    old_keyword_ids = len(game.keyword_ids) if game.keyword_ids else 0
                    old_theme_ids = len(game.theme_ids) if game.theme_ids else 0
                    old_perspective_ids = len(game.perspective_ids) if game.perspective_ids else 0
                    old_developer_ids = len(game.developer_ids) if game.developer_ids else 0
                    old_gamemode_ids = len(game.game_mode_ids) if game.game_mode_ids else 0

                    # Обновляем векторы
                    with transaction.atomic():
                        game.update_materialized_vectors(force=True)
                        game.refresh_from_db()

                    updated_count += 1

                    # Считаем изменения
                    new_engine_ids = len(game.engine_ids) if game.engine_ids else 0
                    new_genre_ids = len(game.genre_ids) if game.genre_ids else 0
                    new_keyword_ids = len(game.keyword_ids) if game.keyword_ids else 0
                    new_theme_ids = len(game.theme_ids) if game.theme_ids else 0
                    new_perspective_ids = len(game.perspective_ids) if game.perspective_ids else 0
                    new_developer_ids = len(game.developer_ids) if game.developer_ids else 0
                    new_gamemode_ids = len(game.game_mode_ids) if game.game_mode_ids else 0

                    if new_engine_ids > old_engine_ids:
                        engine_updates += (new_engine_ids - old_engine_ids)
                    if new_genre_ids > old_genre_ids:
                        genre_updates += (new_genre_ids - old_genre_ids)
                    if new_keyword_ids > old_keyword_ids:
                        keyword_updates += (new_keyword_ids - old_keyword_ids)
                    if new_theme_ids > old_theme_ids:
                        theme_updates += (new_theme_ids - old_theme_ids)
                    if new_perspective_ids > old_perspective_ids:
                        perspective_updates += (new_perspective_ids - old_perspective_ids)
                    if new_developer_ids > old_developer_ids:
                        developer_updates += (new_developer_ids - old_developer_ids)
                    if new_gamemode_ids > old_gamemode_ids:
                        gamemode_updates += (new_gamemode_ids - old_gamemode_ids)

                except Exception as e:
                    error_count += 1
                    self.stdout.write(self.style.ERROR(f'\n❌ Ошибка для игры {game.id}: {str(e)}'))

                pbar.update(1)

        # Финальная статистика
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 ИТОГОВАЯ СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}/{total_games}')
        self.stdout.write(f'❌ Ошибок: {error_count}')

        if engine_updates > 0:
            self.stdout.write(f'⚙️  Добавлено ID движков: {engine_updates}')
        if genre_updates > 0:
            self.stdout.write(f'🎭 Добавлено ID жанров: {genre_updates}')
        if keyword_updates > 0:
            self.stdout.write(f'🔑 Добавлено ID ключевых слов: {keyword_updates}')
        if theme_updates > 0:
            self.stdout.write(f'🎨 Добавлено ID тем: {theme_updates}')
        if perspective_updates > 0:
            self.stdout.write(f'👁️  Добавлено ID перспектив: {perspective_updates}')
        if developer_updates > 0:
            self.stdout.write(f'🏢 Добавлено ID разработчиков: {developer_updates}')
        if gamemode_updates > 0:
            self.stdout.write(f'🎮 Добавлено ID режимов: {gamemode_updates}')

        if error_count == 0:
            self.stdout.write(self.style.SUCCESS('\n✅ Команда выполнена успешно'))
        else:
            self.stdout.write(self.style.WARNING(f'\n⚠️  Команда выполнена с {error_count} ошибками'))