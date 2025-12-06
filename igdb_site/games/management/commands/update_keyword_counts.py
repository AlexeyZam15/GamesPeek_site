# games/management/commands/update_keyword_counts.py
from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from django.utils import timezone
from games.models import Keyword


class Command(BaseCommand):
    help = 'Быстрое обновление cached_usage_count'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch',
            type=int,
            default=1000,
            help='Размер пачки (default: 1000)'
        )

    def handle(self, *args, **options):
        batch_size = options['batch']

        self.stdout.write('🔄 Начинаем обновление cached_usage_count...')

        # 1. Получаем все ключевые слова с подсчетом игр
        keywords = Keyword.objects.all().annotate(
            game_count=Count('game')
        )

        total = keywords.count()
        self.stdout.write(f'📊 Всего ключевых слов: {total}')

        # 2. Обновляем пачками
        batch = []
        for i, keyword in enumerate(keywords.iterator(chunk_size=1000), 1):
            keyword.cached_usage_count = keyword.game_count
            keyword.last_count_update = timezone.now()
            batch.append(keyword)

            # Обновляем пачкой
            if len(batch) >= batch_size:
                Keyword.objects.bulk_update(
                    batch,
                    ['cached_usage_count', 'last_count_update']
                )
                self.stdout.write(f'✅ Обновлено {i}/{total}')
                batch = []

        # Последняя пачка
        if batch:
            Keyword.objects.bulk_update(
                batch,
                ['cached_usage_count', 'last_count_update']
            )

        # 3. Статистика (ИСПРАВЛЕННЫЙ СИНТАКСИС)
        stats = Keyword.objects.aggregate(
            total=Count('id'),
            zero=Count('id', filter=Q(cached_usage_count=0)),
            non_zero=Count('id', filter=Q(cached_usage_count__gt=0)),
        )

        self.stdout.write('\n📈 Статистика:')
        self.stdout.write(f'   Всего: {stats["total"]}')
        self.stdout.write(f'   С нулевым счетчиком: {stats["zero"]}')
        self.stdout.write(f'   С ненулевым счетчиком: {stats["non_zero"]}')

        self.stdout.write(self.style.SUCCESS('\n✅ Обновление завершено!'))