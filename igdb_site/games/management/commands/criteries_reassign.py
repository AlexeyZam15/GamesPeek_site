"""
Команда для переназначения тем в жанры и ключевых слов в темы
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Game, Theme, Genre, Keyword
import sys


class Command(BaseCommand):
    help = 'Перенести темы в жанры и ключевые слова в темы'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет сделано без изменения БД'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Размер батча для обработки (по умолчанию: 1000)'
        )
        parser.add_argument(
            '--keep-old',
            action='store_true',
            help='Не удалять старые критерии'
        )

    def _rename_genre(self, old_name, new_name, dry_run, batch_size):
        """
        Переименовывает жанр, обновляя все связи с играми.

        Аргументы:
            old_name: текущее название жанра
            new_name: новое название жанра
            dry_run: если True, только показывает что будет сделано
            batch_size: размер батча для обработки

        Возвращает:
            tuple: (был ли жанр переименован, количество обновленных игр)
        """
        self.stdout.write(f'\n=== ПЕРЕИМЕНОВАНИЕ ЖАНРА "{old_name}" -> "{new_name}" ===')

        try:
            old_genre = Genre.objects.filter(name__iexact=old_name).first()

            if not old_genre:
                self.stdout.write(self.style.WARNING(f'Жанр "{old_name}" не найден в базе данных'))
                return False, 0

            # Проверяем, существует ли уже жанр с новым названием
            existing_genre = Genre.objects.filter(name__iexact=new_name).first()

            if existing_genre:
                self.stdout.write(self.style.WARNING(f'Жанр "{new_name}" уже существует (ID: {existing_genre.id})'))
                self.stdout.write(f'Будет выполнено объединение жанров: "{old_name}" -> "{new_name}"')

                # Получаем игры со старым жанром
                games_with_old_genre = Game.objects.filter(genres=old_genre)
                games_count = games_with_old_genre.count()

                if games_count == 0:
                    self.stdout.write(f'Старый жанр "{old_name}" не используется')
                    if not dry_run:
                        old_genre.delete()
                        self.stdout.write(self.style.SUCCESS(f'Жанр "{old_name}" удален'))
                    return True, 0

                self.stdout.write(f'Найдено {games_count} игр со старым жанром "{old_name}"')

                if not dry_run:
                    # Получаем ID всех игр со старым жанром
                    game_ids = list(games_with_old_genre.values_list('id', flat=True))
                    games_updated = 0

                    # Добавляем новый жанр к этим играм (батчами)
                    for i in range(0, len(game_ids), batch_size):
                        batch_ids = game_ids[i:i + batch_size]

                        # Получаем игры, у которых еще нет нового жанра
                        existing_relations = set(
                            Game.genres.through.objects.filter(
                                game_id__in=batch_ids,
                                genre_id=existing_genre.id
                            ).values_list('game_id', flat=True)
                        )

                        new_relations = [
                            Game.genres.through(game_id=game_id, genre_id=existing_genre.id)
                            for game_id in batch_ids
                            if game_id not in existing_relations
                        ]

                        if new_relations:
                            Game.genres.through.objects.bulk_create(
                                new_relations,
                                ignore_conflicts=True
                            )

                        # Удаляем старый жанр у этих игр
                        deleted_count, _ = Game.genres.through.objects.filter(
                            game_id__in=batch_ids,
                            genre_id=old_genre.id
                        ).delete()
                        games_updated += deleted_count

                        current = min(i + batch_size, len(game_ids))
                        self.print_progress_bar(
                            current, len(game_ids),
                            prefix=f'Объединение жанров "{old_name}" -> "{new_name}"',
                            suffix=f'Обработано игр: {games_updated}'
                        )

                    print()

                    # Удаляем старый жанр
                    old_genre.delete()
                    self.stdout.write(self.style.SUCCESS(
                        f'Жанр "{old_name}" объединен с "{new_name}". Обновлено игр: {games_updated}'
                    ))
                    return True, games_updated
                else:
                    self.stdout.write(self.style.WARNING(
                        f'[DRY-RUN] Будет объединен жанр "{old_name}" с "{new_name}" для {games_count} игр'
                    ))
                    return True, games_count

            else:
                # Нового жанра не существует - просто переименовываем
                self.stdout.write(f'Переименование жанра "{old_name}" в "{new_name}"')

                games_count = Game.objects.filter(genres=old_genre).count()

                if not dry_run:
                    old_genre.name = new_name
                    old_genre.save()
                    self.stdout.write(self.style.SUCCESS(
                        f'Жанр "{old_name}" переименован в "{new_name}". Затронуто игр: {games_count}'
                    ))
                    return True, games_count
                else:
                    self.stdout.write(self.style.WARNING(
                        f'[DRY-RUN] Будет переименован жанр "{old_name}" -> "{new_name}" для {games_count} игр'
                    ))
                    return True, games_count

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка при переименовании жанра: {e}'))
            return False, 0

    def _process_themes_to_genres(self, theme_to_genre_mapping, dry_run, batch_size, keep_old):
        """
        Перенос тем в жанры.
        При keep_old=False полностью удаляет темы и все их связи с играми.
        """
        self.stdout.write('=== ПЕРЕНОС ТЕМ В ЖАНРЫ ===')

        # Приводим маппинг к нижнему регистру для регистронезависимого сравнения
        mapping_lower = {k.lower(): v for k, v in theme_to_genre_mapping.items()}

        # Получаем список тем для полного удаления
        themes_to_remove = self.get_themes_to_remove()
        themes_to_remove_lower = [theme.lower() for theme in themes_to_remove]

        # Выводим информацию о том, какие темы ищем
        self.stdout.write(f'Ищем темы для переноса: {list(theme_to_genre_mapping.keys())}')
        if themes_to_remove:
            self.stdout.write(f'Темы для полного удаления: {themes_to_remove}')

        # Получаем все темы и фильтруем по нижнему регистру
        all_themes = Theme.objects.all()
        themes_to_process = []
        for theme in all_themes:
            theme_lower = theme.name.lower()
            if theme_lower in mapping_lower:
                themes_to_process.append(theme)
                self.stdout.write(f'Найдена тема для переноса: "{theme.name}" (ID: {theme.id})')

        if not themes_to_process:
            self.stdout.write(self.style.WARNING('Не найдено ни одной темы для переноса'))

        # Кешируем все жанры заранее
        genres_cache = {}
        for theme_name, genre_name in theme_to_genre_mapping.items():
            genre, created = Genre.objects.get_or_create(
                name=genre_name,
                defaults={'igdb_id': -abs(hash(genre_name)) % 1000000}
            )
            genres_cache[theme_name] = genre
            if created:
                self.stdout.write(f'Создан новый жанр: "{genre_name}" (ID: {genre.id})')
            else:
                self.stdout.write(f'Используется существующий жанр: "{genre_name}" (ID: {genre.id})')

        total_added_genres = 0
        total_removed_themes = 0

        # Обработка тем из маппинга
        for theme in themes_to_process:
            theme_name_lower = theme.name.lower()
            # Находим оригинальный ключ в маппинге
            original_key = None
            for key in theme_to_genre_mapping.keys():
                if key.lower() == theme_name_lower:
                    original_key = key
                    break

            if not original_key:
                self.stdout.write(self.style.WARNING(f'Не найден оригинальный ключ для темы "{theme.name}"'))
                continue

            genre = genres_cache.get(original_key)

            if not genre:
                self.stdout.write(self.style.WARNING(f'Не найден жанр для ключа "{original_key}"'))
                continue

            # Получаем ID игр с этой темой
            game_ids = list(Game.objects.filter(themes=theme).values_list('id', flat=True))

            if not game_ids:
                self.stdout.write(self.style.WARNING(f'Для темы "{theme.name}" нет игр'))
                # Если нет игр и keep_old=False, удаляем тему
                if not keep_old and not dry_run:
                    theme.delete()
                    self.stdout.write(f'Тема "{theme.name}" удалена (нет связанных игр)')
                    total_removed_themes += 1
                continue

            total_games = len(game_ids)
            added_genres = 0
            removed_themes = 0

            self.stdout.write(self.style.NOTICE(f'Тема "{theme.name}" -> жанр "{genre.name}": {total_games} игр'))

            # Обрабатываем батчами с bulk операциями
            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    # Bulk добавление жанров (только тех, у кого еще нет)
                    existing_genre_ids = set(
                        Game.genres.through.objects.filter(
                            game_id__in=batch_ids,
                            genre_id=genre.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.genres.through(game_id=game_id, genre_id=genre.id)
                        for game_id in batch_ids
                        if game_id not in existing_genre_ids
                    ]

                    if new_relations:
                        Game.genres.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_genres += len(new_relations)
                        self.stdout.write(f'  Добавлено {len(new_relations)} связей с жанром "{genre.name}"')

                    # Bulk удаление тем (если не keep-old)
                    if not keep_old:
                        # Сначала получаем количество связей до удаления
                        before_count = Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).count()

                        deleted_count, _ = Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).delete()

                        removed_themes += deleted_count

                        if deleted_count > 0:
                            self.stdout.write(
                                f'  Удалено {deleted_count} связей с темой "{theme.name}" (было {before_count})')
                else:
                    # Для dry-run просто считаем
                    added_genres += len(batch_ids)
                    if not keep_old:
                        removed_themes += len(batch_ids)

                # Обновляем прогресс-бар
                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{theme.name[:15]}..."',
                    suffix=f'Добавлено: {added_genres}, Удалено: {removed_themes}'
                )

            print()

            # После обработки всех батчей, если тема больше не используется, удаляем её
            if not keep_old and not dry_run:
                remaining_games = Game.objects.filter(themes=theme).count()
                if remaining_games == 0:
                    theme.delete()
                    self.stdout.write(self.style.SUCCESS(f'  Тема "{theme.name}" полностью удалена из базы данных'))
                else:
                    self.stdout.write(
                        self.style.WARNING(f'  Тема "{theme.name}" всё ещё используется в {remaining_games} играх'))

            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено жанров: {added_genres}, Удалено связей с темой: {removed_themes}'
            ))

            total_added_genres += added_genres
            total_removed_themes += removed_themes

        # Дополнительная проверка: удаляем темы, которые указаны в get_themes_to_remove()
        # и не были обработаны в основном цикле
        if not keep_old and not dry_run and themes_to_remove:
            self.stdout.write('\n=== УДАЛЕНИЕ ДОПОЛНИТЕЛЬНЫХ ТЕМ ===')

            for theme_name_to_remove in themes_to_remove:
                theme_name_lower = theme_name_to_remove.lower()

                # Проверяем, не была ли эта тема уже обработана в маппинге
                if theme_name_lower not in mapping_lower:
                    existing_theme = Theme.objects.filter(name__iexact=theme_name_to_remove).first()
                    if existing_theme:
                        self.stdout.write(f'\nУдаление темы "{existing_theme.name}" (ID: {existing_theme.id})')

                        # Получаем все игры с этой темой
                        games_with_theme = Game.objects.filter(themes=existing_theme)
                        games_count = games_with_theme.count()

                        if games_count > 0:
                            self.stdout.write(f'Тема используется в {games_count} играх')
                            game_ids = list(games_with_theme.values_list('id', flat=True))
                            removed_relations = 0

                            # Удаляем связи батчами
                            for i in range(0, len(game_ids), batch_size):
                                batch_ids = game_ids[i:i + batch_size]
                                deleted_count, _ = Game.themes.through.objects.filter(
                                    game_id__in=batch_ids,
                                    theme_id=existing_theme.id
                                ).delete()
                                removed_relations += deleted_count

                                current = min(i + batch_size, len(game_ids))
                                self.print_progress_bar(
                                    current, len(game_ids),
                                    prefix=f'Удаление связей темы "{theme_name_to_remove}"',
                                    suffix=f'Удалено связей: {removed_relations}'
                                )

                            print()
                            self.stdout.write(
                                self.style.SUCCESS(f'Удалено {removed_relations} связей темы "{theme_name_to_remove}"'))

                        # Удаляем саму тему
                        existing_theme.delete()
                        self.stdout.write(
                            self.style.SUCCESS(f'Тема "{theme_name_to_remove}" полностью удалена из базы данных'))
                        total_removed_themes += 1
                    else:
                        self.stdout.write(self.style.WARNING(f'Тема "{theme_name_to_remove}" не найдена в базе данных'))

        return total_added_genres, total_removed_themes

    def _process_keywords_to_themes(self, keyword_to_theme_mapping, dry_run, batch_size, keep_old):
        """Перенос ключевых слов в темы"""
        self.stdout.write('\n=== ПЕРЕНОС КЛЮЧЕВЫХ СЛОВ В ТЕМЫ ===')

        # Приводим маппинг к нижнему регистру для регистронезависимого сравнения
        mapping_lower = {k.lower(): v for k, v in keyword_to_theme_mapping.items()}

        # Получаем все ключевые слова и фильтруем по нижнему регистру
        all_keywords = Keyword.objects.all()
        keywords_to_process = []
        for keyword in all_keywords:
            if keyword.name.lower() in mapping_lower:
                keywords_to_process.append(keyword)

        # Кешируем все темы заранее
        themes_cache = {}
        for keyword_name, theme_name in keyword_to_theme_mapping.items():
            theme, created = Theme.objects.get_or_create(
                name=theme_name,
                defaults={'igdb_id': -abs(hash(theme_name)) % 1000000}
            )
            themes_cache[keyword_name] = theme
            if created:
                self.stdout.write(f'Создана новая тема: "{theme_name}"')

        total_added_themes = 0
        total_removed_keywords = 0

        for keyword in keywords_to_process:
            keyword_name_lower = keyword.name.lower()
            # Находим оригинальный ключ в маппинге
            original_key = None
            for key in keyword_to_theme_mapping.keys():
                if key.lower() == keyword_name_lower:
                    original_key = key
                    break

            if not original_key:
                continue

            theme = themes_cache.get(original_key)

            if not theme:
                continue

            # Получаем ID игр с этим ключевым словом
            game_ids = list(Game.objects.filter(keywords=keyword).values_list('id', flat=True))

            if not game_ids:
                continue

            total_games = len(game_ids)
            added_themes = 0
            removed_keywords = 0

            self.stdout.write(f'Ключ.слово "{keyword.name}" -> тема "{theme.name}": {total_games} игр')

            # Обрабатываем батчами с bulk операциями
            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    # Bulk добавление тем (только тех, у кого еще нет)
                    existing_theme_ids = set(
                        Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.themes.through(game_id=game_id, theme_id=theme.id)
                        for game_id in batch_ids
                        if game_id not in existing_theme_ids
                    ]

                    if new_relations:
                        Game.themes.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_themes += len(new_relations)

                    # Ключевые слова НЕ УДАЛЯЕМ, даже если keep_old = False
                    # Удаление убрано по требованию
                else:
                    # Для dry-run просто считаем
                    added_themes += len(batch_ids)

                # Обновляем прогресс-бар
                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{keyword.name}"',
                    suffix=f'Добавлено: {added_themes}'
                )

            print()
            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено тем: {added_themes}, Ключ.слова сохранены'
            ))

            total_added_themes += added_themes

        return total_added_themes, total_removed_keywords

    def _process_themes_to_keywords(self, theme_to_keyword_mapping, dry_run, batch_size, keep_old):
        """Перенос тем в ключевые слова"""
        self.stdout.write('\n=== ПЕРЕНОС ТЕМ В КЛЮЧЕВЫЕ СЛОВА ===')

        # Приводим маппинг к нижнему регистру для регистронезависимого сравнения
        mapping_lower = {k.lower(): v for k, v in theme_to_keyword_mapping.items()}

        # Получаем все темы и фильтруем по нижнему регистру
        all_themes = Theme.objects.all()
        themes_to_process = []
        for theme in all_themes:
            if theme.name.lower() in mapping_lower:
                themes_to_process.append(theme)

        keywords_cache = {}
        for theme_name, keyword_name in theme_to_keyword_mapping.items():
            keyword, created = Keyword.objects.get_or_create(
                name=keyword_name,
                defaults={'igdb_id': -abs(hash(keyword_name)) % 1000000}
            )
            keywords_cache[theme_name] = keyword
            if created:
                self.stdout.write(f'Создано новое ключ.слово: "{keyword_name}"')

        total_added_keywords = 0
        total_removed_themes = 0

        for theme in themes_to_process:
            theme_name_lower = theme.name.lower()
            # Находим оригинальный ключ в маппинге
            original_key = None
            for key in theme_to_keyword_mapping.keys():
                if key.lower() == theme_name_lower:
                    original_key = key
                    break

            if not original_key:
                continue

            keyword = keywords_cache.get(original_key)

            if not keyword:
                continue

            game_ids = list(Game.objects.filter(themes=theme).values_list('id', flat=True))

            if not game_ids:
                self.stdout.write(f'Для темы "{theme.name}" нет игр')

                if not keep_old and not dry_run:
                    deleted_count, _ = Theme.objects.filter(id=theme.id).delete()
                    if deleted_count > 0:
                        total_removed_themes += 1
                        self.stdout.write(f'Тема "{theme.name}" удалена (нет связанных игр)')
                continue

            total_games = len(game_ids)
            added_keywords = 0
            removed_themes = 0

            self.stdout.write(f'Тема "{theme.name}" -> ключ.слово "{keyword.name}": {total_games} игр')

            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    existing_keyword_ids = set(
                        Game.keywords.through.objects.filter(
                            game_id__in=batch_ids,
                            keyword_id=keyword.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.keywords.through(game_id=game_id, keyword_id=keyword.id)
                        for game_id in batch_ids
                        if game_id not in existing_keyword_ids
                    ]

                    if new_relations:
                        Game.keywords.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_keywords += len(new_relations)

                    if not keep_old:
                        deleted_count, _ = Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).delete()
                        removed_themes += deleted_count

                        if not Game.objects.filter(themes=theme).exists():
                            theme.delete()
                            self.stdout.write(f'Тема "{theme.name}" полностью удалена')
                else:
                    added_keywords += len(batch_ids)
                    if not keep_old:
                        removed_themes += len(batch_ids)

                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{theme.name}"',
                    suffix=f'Добавлено: {added_keywords}, Удалено: {removed_themes}'
                )

            print()
            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено ключ.слов: {added_keywords}, Удалено тем: {removed_themes}'
            ))

            total_added_keywords += added_keywords
            total_removed_themes += removed_themes

        return total_added_keywords, total_removed_themes

    def _process_genres_to_themes(self, genre_to_theme_mapping, dry_run, batch_size, keep_old):
        """
        Перенос жанров в темы.

        При keep_old=False:
        - переносит жанры в темы
        - удаляет жанры из mapping
        - удаляет жанры, которых нет в PatternManager.GENRE_PATTERNS

        В режиме dry-run:
        - НЕ изменяет БД
        - НО показывает какие жанры будут удалены

        Дополнительно:
        - защищён от ошибки "Model instances must be saved"
        """
        self.stdout.write('\n=== ПЕРЕНОС ЖАНРОВ В ТЕМЫ ===')

        mapping_lower = {k.lower(): v for k, v in genre_to_theme_mapping.items()}

        # Получаем валидные жанры из PatternManager
        try:
            from games.analyze.pattern_manager import PatternManager
            pattern_manager = PatternManager()
            valid_genres = set(pattern_manager.GENRE_PATTERNS.keys())
            valid_genres_lower = {g.lower() for g in valid_genres}
        except (ImportError, AttributeError):
            self.stdout.write(self.style.WARNING('Не удалось получить валидные жанры из PatternManager'))
            valid_genres_lower = set()

        # Получаем все жанры
        all_genres = Genre.objects.all()

        genres_to_process = []
        genres_to_remove = []

        for genre in all_genres:
            # 🔴 защита: пропускаем несохранённые объекты
            if not genre.pk:
                continue

            genre_lower = genre.name.lower()

            if genre_lower in mapping_lower:
                genres_to_process.append(genre)

            if genre_lower not in valid_genres_lower:
                genres_to_remove.append(genre)

        # 🔥 корректный вывод для dry-run
        if genres_to_remove:
            genre_names = [g.name for g in genres_to_remove]

            if dry_run:
                self.stdout.write(self.style.WARNING(
                    f'[DRY-RUN] Будут удалены жанры (не в PatternManager): {genre_names}'
                ))
            else:
                self.stdout.write(
                    f'Жанры для удаления (не в PatternManager): {genre_names}'
                )

        # Кэш тем
        themes_cache = {}
        for genre_name, theme_name in genre_to_theme_mapping.items():
            theme, created = Theme.objects.get_or_create(
                name=theme_name,
                defaults={'igdb_id': -abs(hash(theme_name)) % 1000000}
            )
            themes_cache[genre_name] = theme
            if created:
                self.stdout.write(f'Создана новая тема: "{theme_name}"')

        total_added_themes = 0
        total_removed_genres = 0

        # === ОСНОВНОЙ ПЕРЕНОС ===
        for genre in genres_to_process:
            if not genre.pk:
                continue

            genre_name_lower = genre.name.lower()

            original_key = None
            for key in genre_to_theme_mapping.keys():
                if key.lower() == genre_name_lower:
                    original_key = key
                    break

            if not original_key:
                continue

            theme = themes_cache.get(original_key)
            if not theme:
                continue

            game_ids = list(Game.objects.filter(genres=genre).values_list('id', flat=True))

            if not game_ids:
                self.stdout.write(f'Для жанра "{genre.name}" нет игр')

                if not keep_old and not dry_run:
                    genre.delete()
                    self.stdout.write(f'Жанр "{genre.name}" удален (нет связанных игр)')
                    total_removed_genres += 1
                continue

            total_games = len(game_ids)
            added_themes = 0
            removed_genres = 0

            self.stdout.write(f'Жанр "{genre.name}" -> тема "{theme.name}": {total_games} игр')

            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    existing_theme_ids = set(
                        Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.themes.through(game_id=game_id, theme_id=theme.id)
                        for game_id in batch_ids
                        if game_id not in existing_theme_ids
                    ]

                    if new_relations:
                        Game.themes.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_themes += len(new_relations)

                    if not keep_old:
                        deleted_count, _ = Game.genres.through.objects.filter(
                            game_id__in=batch_ids,
                            genre_id=genre.id
                        ).delete()
                        removed_genres += deleted_count
                else:
                    added_themes += len(batch_ids)
                    if not keep_old:
                        removed_genres += len(batch_ids)

                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{genre.name}"',
                    suffix=f'Добавлено тем: {added_themes}, Удалено связей: {removed_genres}'
                )

            print()

            if not keep_old and not dry_run:
                if not Game.objects.filter(genres=genre).exists():
                    genre.delete()
                    self.stdout.write(self.style.SUCCESS(f'Жанр "{genre.name}" удален'))
                    total_removed_genres += 1

            total_added_themes += added_themes

        # === УДАЛЕНИЕ ЖАНРОВ НЕ ИЗ PATTERN_MANAGER ===
        if not keep_old:
            self.stdout.write('\n=== УДАЛЕНИЕ ЖАНРОВ НЕ ИЗ PATTERN_MANAGER ===')

            for genre in genres_to_remove:
                if not genre.pk:
                    self.stdout.write(self.style.WARNING(
                        f'Пропуск жанра "{genre.name}" — объект не сохранён'
                    ))
                    continue

                if dry_run:
                    self.stdout.write(self.style.WARNING(
                        f'[DRY-RUN] Будет удален жанр "{genre.name}"'
                    ))
                    continue

                self.stdout.write(f'\nУдаление жанра "{genre.name}" (ID: {genre.id})')

                game_ids = list(Game.objects.filter(genres=genre).values_list('id', flat=True))
                removed_relations = 0

                for i in range(0, len(game_ids), batch_size):
                    batch_ids = game_ids[i:i + batch_size]

                    deleted_count, _ = Game.genres.through.objects.filter(
                        game_id__in=batch_ids,
                        genre_id=genre.id
                    ).delete()

                    removed_relations += deleted_count

                    current = min(i + batch_size, len(game_ids))
                    self.print_progress_bar(
                        current, len(game_ids),
                        prefix=f'Удаление "{genre.name}"',
                        suffix=f'Удалено связей: {removed_relations}'
                    )

                print()

                genre.delete()
                self.stdout.write(self.style.SUCCESS(f'Жанр "{genre.name}" полностью удален'))
                total_removed_genres += 1

        return total_added_themes, total_removed_genres

    def _delete_genre_by_name(self, genre_name, dry_run, batch_size, keep_old):
        """
        Полностью удаляет указанный жанр и все связи с ним.

        Аргументы:
            genre_name: название жанра для удаления
            dry_run: если True, только показывает что будет удалено
            batch_size: размер батча для обработки
            keep_old: если True, не удаляет связи (но в данном случае игнорируется)

        Возвращает:
            tuple: (количество удаленных связей, количество удаленных игр с жанром, успешно ли удален жанр)
        """
        self.stdout.write(f'\n=== УДАЛЕНИЕ ЖАНРА "{genre_name}" ===')

        try:
            genre = Genre.objects.filter(name__iexact=genre_name).first()

            if not genre:
                self.stdout.write(self.style.WARNING(f'Жанр "{genre_name}" не найден в базе данных'))
                return 0, 0, False

            self.stdout.write(f'Найден жанр: "{genre.name}" (ID: {genre.id})')

            # Получаем количество игр с этим жанром
            games_with_genre = Game.objects.filter(genres=genre)
            games_count = games_with_genre.count()

            if games_count > 0:
                self.stdout.write(f'Жанр "{genre_name}" используется в {games_count} играх')

                if not dry_run:
                    # Получаем ID всех игр с этим жанром
                    game_ids = list(games_with_genre.values_list('id', flat=True))

                    total_removed_relations = 0

                    # Удаляем связи батчами
                    for i in range(0, len(game_ids), batch_size):
                        batch_ids = game_ids[i:i + batch_size]

                        # Удаляем связи жанра с играми
                        relations_deleted, _ = Game.genres.through.objects.filter(
                            game_id__in=batch_ids,
                            genre_id=genre.id
                        ).delete()

                        total_removed_relations += relations_deleted

                        current = min(i + batch_size, len(game_ids))
                        self.print_progress_bar(
                            current, len(game_ids),
                            prefix=f'Удаление связей жанра "{genre_name}"',
                            suffix=f'Удалено связей: {total_removed_relations}'
                        )

                    print()
                    self.stdout.write(
                        self.style.SUCCESS(f'Удалено {total_removed_relations} связей жанра "{genre_name}" с играми'))

                    # Удаляем сам жанр
                    genre.delete()
                    self.stdout.write(self.style.SUCCESS(f'Жанр "{genre_name}" полностью удален из базы данных'))

                    return total_removed_relations, games_count, True
                else:
                    # Режим dry-run: показываем что будет удалено
                    self.stdout.write(
                        self.style.WARNING(f'[DRY-RUN] Будет удалено {games_count} игр из жанра "{genre_name}"'))
                    self.stdout.write(self.style.WARNING(f'[DRY-RUN] Будет удален жанр "{genre_name}"'))
                    return 0, games_count, True
            else:
                # Жанр не используется, можно удалить сразу
                self.stdout.write(f'Жанр "{genre_name}" не используется ни в одной игре')

                if not dry_run:
                    genre.delete()
                    self.stdout.write(self.style.SUCCESS(f'Жанр "{genre_name}" удален (не использовался)'))
                    return 0, 0, True
                else:
                    self.stdout.write(self.style.WARNING(f'[DRY-RUN] Будет удален жанр "{genre_name}"'))
                    return 0, 0, True

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка при удалении жанра "{genre_name}": {e}'))
            return 0, 0, False

    def _ensure_all_genres_exist(self, pattern_manager, dry_run):
        """
        Создает все жанры, которые определены в PatternManager.GENRE_PATTERNS,
        если они еще не существуют в базе данных.

        Аргументы:
            pattern_manager: экземпляр PatternManager
            dry_run: если True, только показывает что будет создано

        Возвращает:
            int: количество созданных жанров
        """
        self.stdout.write('\n=== СОЗДАНИЕ НЕДОСТАЮЩИХ ЖАНРОВ ИЗ PATTERN_MANAGER ===')

        genre_patterns = getattr(pattern_manager, 'GENRE_PATTERNS', {})
        if not genre_patterns:
            self.stdout.write(self.style.WARNING('Не найдены паттерны жанров в PatternManager'))
            return 0

        created_count = 0
        existing_count = 0

        for genre_name in genre_patterns.keys():
            # Проверяем, существует ли уже жанр
            existing_genre = Genre.objects.filter(name__iexact=genre_name).first()

            if existing_genre:
                existing_count += 1
                self.stdout.write(f'Жанр "{genre_name}" уже существует (ID: {existing_genre.id})')
            else:
                self.stdout.write(f'Создание жанра: "{genre_name}"')
                if not dry_run:
                    self.create_genre_with_igdb_id(genre_name)
                    created_count += 1
                else:
                    created_count += 1

        self.stdout.write(self.style.SUCCESS(
            f'Жанров в PatternManager: {len(genre_patterns)}, '
            f'существующих: {existing_count}, '
            f'{"создано" if not dry_run else "будет создано"}: {created_count}'
        ))

        return created_count

    def _ensure_all_themes_exist(self, pattern_manager, dry_run):
        """
        Создает все темы, которые определены в PatternManager.THEME_PATTERNS,
        если они еще не существуют в базе данных.

        Аргументы:
            pattern_manager: экземпляр PatternManager
            dry_run: если True, только показывает что будет создано

        Возвращает:
            int: количество созданных тем
        """
        self.stdout.write('\n=== СОЗДАНИЕ НЕДОСТАЮЩИХ ТЕМ ИЗ PATTERN_MANAGER ===')

        theme_patterns = getattr(pattern_manager, 'THEME_PATTERNS', {})
        if not theme_patterns:
            self.stdout.write(self.style.WARNING('Не найдены паттерны тем в PatternManager'))
            return 0

        created_count = 0
        existing_count = 0

        for theme_name in theme_patterns.keys():
            # Проверяем, существует ли уже тема
            existing_theme = Theme.objects.filter(name__iexact=theme_name).first()

            if existing_theme:
                existing_count += 1
                self.stdout.write(f'Тема "{theme_name}" уже существует (ID: {existing_theme.id})')
            else:
                self.stdout.write(f'Создание темы: "{theme_name}"')
                if not dry_run:
                    self.create_theme_with_igdb_id(theme_name)
                    created_count += 1
                else:
                    created_count += 1

        self.stdout.write(self.style.SUCCESS(
            f'Тем в PatternManager: {len(theme_patterns)}, '
            f'существующих: {existing_count}, '
            f'{"создано" if not dry_run else "будет создано"}: {created_count}'
        ))

        return created_count

    def get_themes_to_remove(self):
        """
        Возвращает список тем, которые нужно полностью удалить из базы данных.
        Удаляются темы, которые НЕ присутствуют в pattern_manager.THEME_PATTERNS.
        """
        try:
            from games.analyze.pattern_manager import PatternManager
            pattern_manager = PatternManager()
            valid_themes = set(pattern_manager.THEME_PATTERNS.keys())
        except (ImportError, AttributeError):
            self.stdout.write(
                self.style.WARNING('Не удалось получить валидные темы из PatternManager, удаление отменено'))
            return []

        # Получаем все темы из базы данных
        all_themes = Theme.objects.values_list('name', flat=True)

        # Возвращаем темы, которых нет в валидном списке
        themes_to_remove = [theme for theme in all_themes if theme not in valid_themes]

        self.stdout.write(f'Найдено {len(themes_to_remove)} тем для удаления (из {len(all_themes)} всего)')

        return themes_to_remove

    def get_genre_to_theme_mapping(self):
        """Возвращает маппинг жанров в темы (регистронезависимый)"""
        return {
            'indie': 'Indie',
        }

    def get_theme_to_genre_mapping(self):
        """Возвращает маппинг тем в жанры (регистронезависимый)"""
        return {
            'action': 'Action',
            'open world': 'Open World',
            'sandbox': 'Sandbox',
            'survival': 'Survival',
            'base building': 'Base Building',
            'simulator': 'Simulator',
            'squad management': 'Squad Management',
            'precision combat': 'Precision Combat',  # добавлено
            # 'real-time combat' убрано
        }

    def get_keyword_to_theme_mapping(self):
        """Возвращает маппинг ключевых слов в темы (регистронезависимый)"""
        return {
            'fire emblem': 'Fire Emblem',
        }

    def get_theme_to_keyword_mapping(self):
        """Возвращает маппинг тем в ключевые слова (регистронезависимый)"""
        return {
            # 'fire emblem': 'Fire Emblem',  # Удалено - теперь переносится из ключевых слов в темы
        }

    def create_genre_with_igdb_id(self, name):
        """Создать жанр с временным igdb_id"""
        igdb_id = -abs(hash(name)) % 1000000

        genre, created = Genre.objects.get_or_create(
            name=name,
            defaults={'igdb_id': igdb_id}
        )
        return genre, created

    def create_theme_with_igdb_id(self, name):
        """Создать тему с временным igdb_id"""
        igdb_id = -abs(hash(name)) % 1000000

        theme, created = Theme.objects.get_or_create(
            name=name,
            defaults={'igdb_id': igdb_id}
        )
        return theme, created

    def print_progress_bar(self, iteration, total, prefix='', suffix='', length=50, fill='█'):
        """Вывести прогресс-бар в одной строке"""
        percent = f"{100 * (iteration / float(total)):.1f}"
        filled_length = int(length * iteration // total)
        bar = fill * filled_length + '-' * (length - filled_length)

        sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {suffix}')
        sys.stdout.flush()

        if iteration == total:
            print()

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        keep_old = options['keep_old']

        # Импортируем PatternManager для получения списков жанров и тем
        try:
            from games.analyze.pattern_manager import PatternManager
            pattern_manager = PatternManager()
        except ImportError:
            self.stdout.write(self.style.ERROR('Не удалось импортировать PatternManager'))
            return

        theme_to_genre_mapping = self.get_theme_to_genre_mapping()
        keyword_to_theme_mapping = self.get_keyword_to_theme_mapping()
        theme_to_keyword_mapping = self.get_theme_to_keyword_mapping()
        genre_to_theme_mapping = self.get_genre_to_theme_mapping()

        total_removed_themes = 0
        total_added_genres = 0
        total_removed_keywords = 0
        total_added_themes = 0
        total_removed_themes_to_keywords = 0
        total_added_keywords = 0
        total_added_themes_from_genres = 0
        total_removed_genres = 0

        # Переменные для отслеживания создания жанров и тем из PatternManager
        created_genres_count = 0
        created_themes_count = 0

        # Переменные для отслеживания удаления жанра Real-time Combat
        genre_to_remove = 'Real-time Combat'
        relations_removed = 0
        games_affected = 0
        genre_removed = False

        # Переменные для отслеживания удаления жанра Hack and slash/Beat 'em up
        genre_hack_and_slash_to_remove = 'Hack and slash/Beat \'em up'
        hack_and_slash_relations_removed = 0
        hack_and_slash_games_affected = 0
        hack_and_slash_genre_removed = False

        with transaction.atomic():
            if dry_run:
                savepoint = transaction.savepoint()

            try:
                # 0. Создание всех недостающих жанров из PatternManager
                created_genres_count = self._ensure_all_genres_exist(pattern_manager, dry_run)

                # 1. Создание всех недостающих тем из PatternManager
                created_themes_count = self._ensure_all_themes_exist(pattern_manager, dry_run)

                # 2. Перенос тем в жанры
                added_genres, removed_themes = self._process_themes_to_genres(
                    theme_to_genre_mapping, dry_run, batch_size, keep_old
                )
                total_added_genres += added_genres
                total_removed_themes += removed_themes

                # 3. Перенос ключевых слов в темы
                added_themes, removed_keywords = self._process_keywords_to_themes(
                    keyword_to_theme_mapping, dry_run, batch_size, keep_old
                )
                total_added_themes += added_themes
                total_removed_keywords += removed_keywords

                # 4. Перенос тем в ключевые слова
                added_keywords, removed_themes_to_keywords = self._process_themes_to_keywords(
                    theme_to_keyword_mapping, dry_run, batch_size, keep_old
                )
                total_added_keywords += added_keywords
                total_removed_themes_to_keywords += removed_themes_to_keywords

                # 5. Перенос жанров в темы (с удалением жанров при keep_old=False)
                added_themes_from_genres, removed_genres = self._process_genres_to_themes(
                    genre_to_theme_mapping, dry_run, batch_size, keep_old
                )
                total_added_themes_from_genres += added_themes_from_genres
                total_removed_genres += removed_genres

                # 6. Удаление жанра "Real-time Combat" (только если keep_old=False)
                if not keep_old:
                    relations_removed, games_affected, genre_removed = self._delete_genre_by_name(
                        genre_to_remove, dry_run, batch_size, keep_old
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING(f'\n=== ЖАНР "{genre_to_remove}" НЕ УДАЛЯЕТСЯ (опция --keep-old) ==='))

                # 7. Удаление жанра "Hack and slash/Beat 'em up" (только если keep_old=False)
                if not keep_old:
                    hack_and_slash_relations_removed, hack_and_slash_games_affected, hack_and_slash_genre_removed = self._delete_genre_by_name(
                        genre_hack_and_slash_to_remove, dry_run, batch_size, keep_old
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING(
                            f'\n=== ЖАНР "{genre_hack_and_slash_to_remove}" НЕ УДАЛЯЕТСЯ (опция --keep-old) ==='))

                if dry_run:
                    transaction.savepoint_rollback(savepoint)

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка при обработке: {e}'))
                if dry_run:
                    transaction.savepoint_rollback(savepoint)
                raise

        self.stdout.write('\n' + '=' * 50)
        if dry_run:
            self.stdout.write(self.style.WARNING('РЕЖИМ ТЕСТИРОВАНИЯ (без изменений в БД)'))
        else:
            self.stdout.write(self.style.SUCCESS('ИЗМЕНЕНИЯ ВНЕСЕНЫ В БД'))

        self.stdout.write(self.style.SUCCESS(f'ИТОГО обработано:'))

        self.stdout.write(self.style.SUCCESS(
            f'  • Создание жанров из PatternManager: {"создано" if not dry_run else "будет создано"} {created_genres_count} жанров'))
        self.stdout.write(self.style.SUCCESS(
            f'  • Создание тем из PatternManager: {"создано" if not dry_run else "будет создано"} {created_themes_count} тем'))

        self.stdout.write(
            self.style.SUCCESS(
                f'  • Темы -> жанры: добавлено {total_added_genres}, удалено тем {total_removed_themes}'))
        self.stdout.write(self.style.SUCCESS(
            f'  • Ключ.слова -> темы: добавлено {total_added_themes}, удалено ключ.слов {total_removed_keywords}'))
        self.stdout.write(self.style.SUCCESS(
            f'  • Темы -> ключ.слова: добавлено {total_added_keywords}, удалено тем {total_removed_themes_to_keywords}'))
        self.stdout.write(self.style.SUCCESS(
            f'  • Жанры -> темы: добавлено {total_added_themes_from_genres}, удалено жанров {total_removed_genres}'))

        # Вывод информации об удалении жанра Real-time Combat (только если keep_old=False)
        if not keep_old:
            if genre_removed:
                if games_affected > 0:
                    self.stdout.write(self.style.SUCCESS(
                        f'  • Удаление жанра "{genre_to_remove}": удалено связей {relations_removed}, '
                        f'затронуто игр {games_affected}'))
                else:
                    self.stdout.write(self.style.SUCCESS(
                        f'  • Удаление жанра "{genre_to_remove}": жанр удален (не использовался)'))
            elif not dry_run:
                self.stdout.write(self.style.WARNING(
                    f'  • Жанр "{genre_to_remove}" не был удален (возможно, не найден)'))

        # Вывод информации об удалении жанра Hack and slash/Beat 'em up (только если keep_old=False)
        if not keep_old:
            if hack_and_slash_genre_removed:
                if hack_and_slash_games_affected > 0:
                    self.stdout.write(self.style.SUCCESS(
                        f'  • Удаление жанра "{genre_hack_and_slash_to_remove}": удалено связей {hack_and_slash_relations_removed}, '
                        f'затронуто игр {hack_and_slash_games_affected}'))
                else:
                    self.stdout.write(self.style.SUCCESS(
                        f'  • Удаление жанра "{genre_hack_and_slash_to_remove}": жанр удален (не использовался)'))
            elif not dry_run:
                self.stdout.write(self.style.WARNING(
                    f'  • Жанр "{genre_hack_and_slash_to_remove}" не был удален (возможно, не найден)'))

        if keep_old:
            self.stdout.write(self.style.WARNING('СТАРЫЕ КРИТЕРИИ СОХРАНЕНЫ (опция --keep-old)'))