# management/commands/replace_plural_keywords.py
import time
from django.core.management.base import BaseCommand
from django.db import transaction, connection
from games.models import Keyword, Game
from games.analyze.keyword_trie import KeywordTrieManager
from tqdm import tqdm
import inflect
from collections import defaultdict


class Command(BaseCommand):
    help = 'Заменяет ключевые слова во множественной форме на единственную (если её нет в базе) - МАССОВАЯ ВЕРСИЯ'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true',
                            help='Только показать, что будет заменено, без реальных изменений')
        parser.add_argument('--min-length', type=int, default=3,
                            help='Минимальная длина слова для проверки (по умолчанию: 3)')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод')
        parser.add_argument('--auto-confirm', action='store_true', help='Автоматически подтвердить замену')
        parser.add_argument('--batch-size', type=int, default=1000, help='Размер батча для массовых операций')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        min_length = options['min_length']
        verbose = options['verbose']
        auto_confirm = options['auto_confirm']
        batch_size = options['batch_size']

        self.stdout.write(self.style.SUCCESS('🔍 Поиск ключевых слов во множественной форме (МАССОВАЯ ВЕРСИЯ)'))
        self.stdout.write('=' * 70)

        # Инициализируем inflect
        try:
            self.inflect_engine = inflect.engine()
            self.stdout.write("✅ Inflect инициализирован")
        except ImportError:
            self.stderr.write(self.style.ERROR("❌ Не установлен модуль inflect. Установите: pip install inflect"))
            return

        # Получаем все ключевые слова одним запросом
        all_keywords = list(Keyword.objects.all().values('id', 'name', 'cached_usage_count', 'category_id'))
        total_keywords = len(all_keywords)

        self.stdout.write(f"📊 Всего ключевых слов в базе: {total_keywords}")
        self.stdout.write('')

        # Создаем маппинг для быстрого поиска
        self.word_to_id = {kw['name'].lower(): kw['id'] for kw in all_keywords}
        self.word_to_usage = {kw['name'].lower(): kw['cached_usage_count'] for kw in all_keywords}
        self.word_to_category = {kw['name'].lower(): kw['category_id'] for kw in all_keywords}

        # Массово находим множественные формы
        plural_map = self._find_plural_keywords_bulk(all_keywords, min_length, verbose)

        if not plural_map:
            self.stdout.write(self.style.SUCCESS('✅ Слова во множественной форме не найдены!'))
            return

        total_plural = len(plural_map)
        self.stdout.write(self.style.WARNING(f"\n⚠️ Найдено слов во множественной форме: {total_plural}"))

        # Группируем по наличию единственной формы
        with_singular = {k: v for k, v in plural_map.items() if v['singular_exists']}
        without_singular = {k: v for k, v in plural_map.items() if not v['singular_exists']}

        self.stdout.write(f"   ↳ С существующей единственной формой: {len(with_singular)}")
        self.stdout.write(f"   ↳ Без единственной формы (будут созданы): {len(without_singular)}")

        if verbose:
            self._show_sample(plural_map)

        if dry_run:
            self.stdout.write(self.style.WARNING('\n🏁 Сухой запуск - ничего не изменено'))
            return

        if not auto_confirm:
            confirm = input(f'\n❓ Заменить {total_plural} слов во множественной форме? (yes/no): ')
            if confirm.lower() != 'yes':
                self.stdout.write(self.style.WARNING('❌ Операция отменена'))
                return

        # Массово выполняем замену
        start_time = time.time()
        stats = self._replace_plural_keywords_bulk(plural_map, batch_size, verbose, dry_run)
        elapsed = time.time() - start_time

        if not dry_run:
            # Очищаем кэш Trie после изменений
            KeywordTrieManager().clear_cache()

        self.stdout.write(self.style.SUCCESS(f'\n✅ Заменено слов: {stats["replaced"]}'))
        self.stdout.write(f"📊 Создано новых единственных форм: {stats['created']}")
        self.stdout.write(f"📊 Перенесено связей с играми: {stats['transferred_relations']}")
        self.stdout.write(f"⏱️ Время выполнения: {elapsed:.1f} сек")
        if stats['replaced'] > 0:
            self.stdout.write(f"⚡ Скорость: {stats['replaced'] / elapsed:.1f} слов/сек")

    def _find_plural_keywords_bulk(self, all_keywords, min_length, verbose):
        """Массово находит множественные формы"""
        plural_map = {}

        for kw in tqdm(all_keywords, desc="Анализ слов", disable=not verbose):
            word_lower = kw['name'].lower()

            if len(word_lower) < min_length:
                continue

            singular_form = self.inflect_engine.singular_noun(word_lower)

            if not singular_form or singular_form == word_lower:
                continue

            singular_exists = singular_form in self.word_to_id

            plural_map[word_lower] = {
                'plural_id': kw['id'],
                'plural_name': kw['name'],
                'plural_usage': kw['cached_usage_count'],
                'singular': singular_form,
                'singular_exists': singular_exists,
                'singular_id': self.word_to_id.get(singular_form),
                'singular_usage': self.word_to_usage.get(singular_form, 0),
                'category_id': kw['category_id']
            }

        return plural_map

    def _show_sample(self, plural_map, limit=10):
        """Показывает примеры найденных слов"""
        self.stdout.write('\n📋 Примеры:')
        for i, (plural, data) in enumerate(list(plural_map.items())[:limit]):
            status = "✅" if data['singular_exists'] else "❌"
            self.stdout.write(f"  {status} {plural} → {data['singular']} (ID: {data['plural_id']})")

    def _replace_plural_keywords_bulk(self, plural_map, batch_size, verbose, dry_run):
        """Массово заменяет множественные формы на единственные"""
        stats = {
            'replaced': 0,
            'created': 0,
            'transferred_relations': 0
        }

        if dry_run:
            stats['replaced'] = len(plural_map)
            stats['created'] = sum(1 for data in plural_map.values() if not data['singular_exists'])
            return stats

        # Группируем по наличию единственной формы
        with_singular = []
        without_singular = []

        for data in plural_map.values():
            if data['singular_exists']:
                with_singular.append(data)
            else:
                without_singular.append(data)

        # 1. Создаём все недостающие единственные формы одним батчем
        if without_singular:
            stats['created'] = self._bulk_create_singular(without_singular, batch_size, verbose)

            # Перезагружаем маппинг после создания
            self._reload_mappings()

        # 2. Собираем все transfers
        all_transfers = []
        all_deletions = []

        for data in tqdm(list(with_singular) + without_singular, desc="Подготовка переноса", disable=not verbose):
            # Определяем target_id
            if data['singular_exists'] or data['singular'] in self.word_to_id:
                target_id = data['singular_id'] or self.word_to_id[data['singular']]

                # Проверяем, что target_id и source_id разные
                if target_id != data['plural_id']:
                    all_transfers.append({
                        'source_id': data['plural_id'],
                        'target_id': target_id
                    })
                    all_deletions.append(data['plural_id'])

        # 3. Массово переносим связи через промежуточную таблицу
        if all_transfers:
            transferred = self._bulk_transfer_relations(all_transfers, batch_size, verbose)
            stats['transferred_relations'] = transferred

        # 4. Массово удаляем исходные слова
        if all_deletions:
            self._bulk_delete_keywords(all_deletions, batch_size, verbose)
            stats['replaced'] = len(all_deletions)

        return stats

    def _bulk_create_singular(self, words_data, batch_size, verbose):
        """Массово создаёт недостающие единственные формы"""
        from django.db.models import Max

        # Получаем максимальный igdb_id
        max_igdb_id = Keyword.objects.aggregate(Max('igdb_id'))['igdb_id__max'] or 1000000

        # Готовим объекты для создания
        to_create = []
        created_count = 0

        # Используем словарь для уникальности
        unique_singular = {}
        for data in words_data:
            singular = data['singular']
            if singular not in unique_singular:
                unique_singular[singular] = {
                    'name': data['singular'],
                    'category_id': data['category_id'],
                    'igdb_id': max_igdb_id + len(unique_singular) + 1
                }

        singular_list = list(unique_singular.values())

        # Создаём батчами
        for i in range(0, len(singular_list), batch_size):
            batch = singular_list[i:i + batch_size]
            keywords_to_create = [
                Keyword(
                    name=item['name'],
                    category_id=item['category_id'],
                    igdb_id=item['igdb_id'],
                    cached_usage_count=0
                )
                for item in batch
            ]

            Keyword.objects.bulk_create(keywords_to_create, batch_size=batch_size)
            created_count += len(keywords_to_create)

            if verbose:
                self.stdout.write(f"   ✨ Создано {len(keywords_to_create)} ключевых слов...")

        return created_count

    def _bulk_transfer_relations(self, transfers, batch_size, verbose):
        """Массово переносит связи с играми"""
        from django.db import connection

        # Получаем имя промежуточной таблицы
        through_table = Game.keywords.through._meta.db_table

        total_transferred = 0

        # Группируем transfers по target_id для массового обновления
        target_to_sources = defaultdict(list)
        for t in transfers:
            target_to_sources[t['target_id']].append(t['source_id'])

        with transaction.atomic():
            for target_id, source_ids in tqdm(target_to_sources.items(), desc="Перенос связей", disable=not verbose):
                # Получаем все связи source_id -> игры
                with connection.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT game_id FROM {through_table}
                        WHERE keyword_id = ANY(%s)
                    """, [source_ids])

                    game_ids = list(set(row[0] for row in cursor.fetchall()))  # Убираем дубликаты

                    if game_ids:
                        # Добавляем новые связи для target_id
                        game_keyword_pairs = [(game_id, target_id) for game_id in game_ids]

                        # Используем INSERT ... ON CONFLICT для PostgreSQL
                        for i in range(0, len(game_keyword_pairs), batch_size):
                            batch_pairs = game_keyword_pairs[i:i + batch_size]
                            cursor.executemany(
                                f"""
                                INSERT INTO {through_table} (game_id, keyword_id)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                                """,
                                batch_pairs
                            )

                        total_transferred += len(game_ids)

                        if verbose and len(game_ids) > 0:
                            self.stdout.write(f"   🔄 Перенесено {len(game_ids)} игр для keyword_id {target_id}")

        return total_transferred

    def _bulk_delete_keywords(self, keyword_ids, batch_size, verbose):
        """Массово удаляет ключевые слова"""
        for i in range(0, len(keyword_ids), batch_size):
            batch = keyword_ids[i:i + batch_size]
            Keyword.objects.filter(id__in=batch).delete()

            if verbose:
                self.stdout.write(f"   🗑️ Удалено {len(batch)} ключевых слов...")

    def _reload_mappings(self):
        """Перезагружает маппинг слов после создания новых"""
        all_keywords = Keyword.objects.all().values('id', 'name')
        self.word_to_id = {kw['name'].lower(): kw['id'] for kw in all_keywords}
        self.word_to_usage = {kw['name'].lower(): 0 for kw in all_keywords}  # Новые слова с 0 использований