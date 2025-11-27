from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from games.models import Keyword, Game


class Command(BaseCommand):
    help = 'Отсев мусорных ключевых слов на основе реального использования'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-usage',
            type=int,
            default=2,
            help='Минимальное количество использований ключевого слова'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            default=False,
            help='Показать какие ключевые слова будут удалены без фактического удаления'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            default=False,
            help='Принудительное удаление без подтверждения'
        )
        parser.add_argument(
            '--skip-counter-update',
            action='store_true',
            default=False,
            help='Пропустить обновление счетчиков для ускорения'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Размер батча для отображения прогресса'
        )
        parser.add_argument(
            '--include',
            type=str,
            help='Удалить все ключевые слова, где в названии содержится эта строка (например: "Keyword")'
        )
        parser.add_argument(
            '--show-games',
            action='store_true',
            default=False,
            help='Показать в каких играх используются ключевые слова'
        )

    def handle(self, *args, **options):
        min_usage = options['min_usage']
        dry_run = options['dry_run']
        force = options['force']
        skip_counter_update = options['skip_counter_update']
        batch_size = options['batch_size']
        include_pattern = options['include']
        show_games = options['show_games']

        self.stdout.write(
            self.style.SUCCESS(f'🔍 Поиск мусорных ключевых слов (min_usage={min_usage})')
        )

        if include_pattern:
            self.stdout.write(
                self.style.WARNING(f'🎯 Будут удалены все ключевые слова содержащие: "{include_pattern}"')
            )

        if show_games:
            self.stdout.write('🎮 Будут показаны игры для каждого ключевого слова')

        # Обновляем счетчики только если не пропущено
        if not skip_counter_update:
            self.stdout.write('🔄 Обновление счетчиков использования...')
            updated_count = self.update_usage_counts_with_progress(dry_run, batch_size)
        else:
            self.stdout.write('⏭️  Пропущено обновление счетчиков')
            updated_count = 0

        # Находим ключевые слова для удаления
        self.stdout.write('📊 Поиск ключевых слов для удаления...')

        # Базовый запрос
        base_query = Q(real_usage_count__lt=min_usage)

        # Если указан include, ищем по содержанию в названии
        if include_pattern:
            base_query = Q(name__icontains=include_pattern)
        else:
            # Иначе используем стандартный паттерн Keyword чисел
            base_query &= Q(name__iregex=r'^keyword \d+$')

        keywords_with_usage = Keyword.objects.annotate(
            real_usage_count=Count('game')
        ).filter(base_query)

        will_be_deleted_count = keywords_with_usage.count()
        will_be_deleted = list(keywords_with_usage)

        self.stdout.write(
            self.style.WARNING(f'📊 Найдено кандидатов: {will_be_deleted_count}')
        )
        self.stdout.write(
            self.style.SUCCESS(f'✅ БУДЕТ УДАЛЕНО: {will_be_deleted_count} ключевых слов')
        )

        # Показываем статистику
        self.show_real_stats(min_usage, include_pattern)

        # В DRY-RUN режиме показываем полный список
        if dry_run:
            if will_be_deleted_count > 0:
                self.stdout.write(
                    self.style.WARNING('\n🎯 ПОЛНЫЙ СПИСОК КЛЮЧЕВЫХ СЛОВ ДЛЯ УДАЛЕНИЯ:')
                )

                # Группируем по использованию
                usage_groups = {}
                for keyword in sorted(will_be_deleted, key=lambda x: (x.real_usage_count, x.name)):
                    usage_count = keyword.real_usage_count
                    if usage_count not in usage_groups:
                        usage_groups[usage_count] = []
                    usage_groups[usage_count].append(keyword)

                for usage_count, keywords in sorted(usage_groups.items()):
                    self.stdout.write(f'\n📊 Использований: {usage_count} ({len(keywords)} ключевых слов):')
                    for keyword in keywords:
                        self.stdout.write(
                            f"   🗑️  {keyword.name} (ID: {keyword.igdb_id}, Категория: {keyword.category.name if keyword.category else 'Нет'})")

                        # Показываем игры если включена опция
                        if show_games and usage_count > 0:
                            games = keyword.game_set.all()[:3]  # Показываем первые 3 игры
                            if games:
                                game_names = [game.name for game in games]
                                self.stdout.write(f"      🎮 Используется в: {', '.join(game_names)}")
                                if usage_count > 3:
                                    self.stdout.write(f"      ... и еще {usage_count - 3} игр")

                self.stdout.write(
                    self.style.SUCCESS(f'\n📋 Всего будет удалено: {will_be_deleted_count} ключевых слов')
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS('✅ Нет ключевых слов для удаления!')
                )

            self.stdout.write(
                self.style.SUCCESS('\n✅ Dry-run завершен. Для удаления запустите без --dry-run')
            )
            return

        # Реальное выполнение
        if will_be_deleted_count > 0 and (force or self.confirm_deletion(will_be_deleted_count, include_pattern)):
            deleted_count = 0
            self.stdout.write(f'🗑️  Начинаю удаление {will_be_deleted_count} ключевых слов...')

            for i, keyword in enumerate(will_be_deleted, 1):
                keyword_name = keyword.name
                keyword_id = keyword.igdb_id
                category_name = keyword.category.name if keyword.category else "Без категории"

                # Показываем игры перед удалением если включена опция
                if show_games and keyword.real_usage_count > 0:
                    games = keyword.game_set.all()[:2]  # Показываем первые 2 игры
                    if games:
                        game_names = [game.name for game in games]
                        self.stdout.write(f"      🎮 Удаляется из игр: {', '.join(game_names)}")
                        if keyword.real_usage_count > 2:
                            self.stdout.write(f"      ... и еще {keyword.real_usage_count - 2} игр")

                keyword.delete()
                deleted_count += 1

                # Показываем прогресс каждые 10 ключевых слов или для всех если их мало
                if will_be_deleted_count <= 50 or i % 10 == 0 or i == will_be_deleted_count:
                    self.stdout.write(f"  [{i}/{will_be_deleted_count}] Удалено: {keyword_name}")

            self.stdout.write(
                self.style.SUCCESS(f'✅ Успешно удалено {deleted_count} ключевых слов!')
            )
        else:
            self.stdout.write('❌ Удаление отменено.')

    def update_usage_counts_with_progress(self, dry_run=False, batch_size=100):
        """Обновляет счетчики использования с отображением прогресса"""
        all_keywords = Keyword.objects.all()
        total_keywords = all_keywords.count()
        updated_count = 0

        self.stdout.write(f'📊 Всего ключевых слов для проверки: {total_keywords}')

        for i, keyword in enumerate(all_keywords, 1):
            old_count = keyword.usage_count
            new_count = keyword.game_set.count()

            if old_count != new_count:
                if not dry_run:
                    # В реальном режиме - сохраняем изменения
                    keyword.usage_count = new_count
                    keyword.popularity_score = new_count
                    keyword.save()

                updated_count += 1

                # Показываем прогресс каждые batch_size ключевых слов
                if i % batch_size == 0 or i == total_keywords:
                    progress = f"[{i}/{total_keywords}]"
                    status = "📝 [DRY-RUN]" if dry_run else "🔄"
                    self.stdout.write(f"  {progress} {status} Обновлено: {updated_count} ключевых слов")

                # Показываем детали только для первых 20 изменений
                if updated_count <= 20:
                    status = "📝 [DRY-RUN] Изменено" if dry_run else "🔄 Изменено"
                    self.stdout.write(f"    {status}: {keyword.name} ({old_count} → {new_count})")

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(f'📊 [DRY-RUN] Будет обновлено: {updated_count} ключевых слов')
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f'✅ Обновлено: {updated_count} ключевых слов')
            )

        return updated_count

    def show_real_stats(self, min_usage, include_pattern):
        """Показывает реальную статистику использования"""
        keywords_with_usage = Keyword.objects.annotate(
            real_usage_count=Count('game')
        )

        total_keywords = keywords_with_usage.count()
        zero_usage = keywords_with_usage.filter(real_usage_count=0).count()
        low_usage = keywords_with_usage.filter(real_usage_count__lt=min_usage).count()

        if include_pattern:
            # Статистика для включенного паттерна
            pattern_count = keywords_with_usage.filter(name__icontains=include_pattern).count()
            pattern_zero_usage = keywords_with_usage.filter(
                name__icontains=include_pattern,
                real_usage_count=0
            ).count()
            pattern_low_usage = keywords_with_usage.filter(
                name__icontains=include_pattern,
                real_usage_count__lt=min_usage
            ).count()
        else:
            # Статистика для Keyword чисел
            pattern_count = keywords_with_usage.filter(name__iregex=r'^keyword \d+$').count()
            pattern_zero_usage = keywords_with_usage.filter(
                name__iregex=r'^keyword \d+$',
                real_usage_count=0
            ).count()
            pattern_low_usage = keywords_with_usage.filter(
                name__iregex=r'^keyword \d+$',
                real_usage_count__lt=min_usage
            ).count()

        self.stdout.write('\n📈 РЕАЛЬНАЯ СТАТИСТИКА:')
        self.stdout.write(f"  Всего ключевых слов: {total_keywords}")
        self.stdout.write(f"  Никогда не использовались: {zero_usage}")
        self.stdout.write(f"  Использовались менее {min_usage} раз: {low_usage}")

        if include_pattern:
            self.stdout.write(f"  Содержат '{include_pattern}': {pattern_count}")
            self.stdout.write(f"  Содержат '{include_pattern}' и не использовались: {pattern_zero_usage}")
            self.stdout.write(f"  Содержат '{include_pattern}' и мало использовались: {pattern_low_usage}")
        else:
            self.stdout.write(f"  Ключевые слова 'Keyword XXX': {pattern_count}")
            self.stdout.write(f"  'Keyword XXX' и не использовались: {pattern_zero_usage}")
            self.stdout.write(f"  'Keyword XXX' и мало использовались: {pattern_low_usage}")

    def confirm_deletion(self, count, include_pattern):
        """Запрос подтверждения удаления"""
        if include_pattern:
            message = f'Будет удалено {count} ключевых слов содержащих "{include_pattern}"!'
        else:
            message = f'Будет удалено {count} ключевых слов с названиями "Keyword XXX"!'

        self.stdout.write(
            self.style.ERROR(f'\n⚠️  ВНИМАНИЕ: {message}')
        )
        confirmation = input('Продолжить? (y/N): ')
        return confirmation.lower() in ('y', 'yes', 'д', 'да')