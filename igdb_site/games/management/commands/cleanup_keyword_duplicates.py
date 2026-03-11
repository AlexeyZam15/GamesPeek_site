# management/commands/cleanup_keyword_duplicates.py
from django.core.management.base import BaseCommand
from django.db.models import Count
from games.models import Keyword
from games.analyze.keyword_trie import KeywordTrieManager
from tqdm import tqdm


class Command(BaseCommand):
    help = 'Находит и удаляет ключевые слова-дубли (производные формы от основных слов)'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Только показать, что будет удалено, без удаления')
        parser.add_argument('--min-length', type=int, default=3,
                            help='Минимальная длина слова для проверки (по умолчанию: 3)')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод')
        parser.add_argument('--auto-confirm', action='store_true', help='Автоматически подтвердить удаление')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        min_length = options['min_length']
        verbose = options['verbose']
        auto_confirm = options['auto_confirm']

        self.stdout.write(self.style.SUCCESS('🔍 Поиск ключевых слов-дублей'))
        self.stdout.write('=' * 70)

        # Получаем все ключевые слова
        all_keywords = Keyword.objects.all().order_by('name')
        total_keywords = all_keywords.count()

        self.stdout.write(f"📊 Всего ключевых слов в базе: {total_keywords}")
        self.stdout.write('')

        # Группируем слова по возможным производным формам
        duplicates = self._find_duplicates(all_keywords, min_length, verbose)

        if not duplicates:
            self.stdout.write(self.style.SUCCESS('✅ Дубли не найдены!'))
            return

        # Показываем статистику
        total_duplicates = sum(len(data['duplicates']) for data in duplicates.values())
        self.stdout.write(self.style.WARNING(f"\n⚠️ Найдено потенциальных дублей: {total_duplicates}"))
        self.stdout.write('')

        # Показываем детали
        for base_word, data in list(duplicates.items())[:20]:  # Показываем первые 20
            self.stdout.write(f"📌 Основа: {base_word} (ID: {data['base_id']})")
            self.stdout.write(f"   Использований в играх: {data['base_usage']}")

            # Сортируем дубли по убыванию использования
            sorted_dupes = sorted(data['duplicates'], key=lambda x: x['usage'], reverse=True)

            for dup in sorted_dupes:
                usage_info = f" (использований: {dup['usage']})" if dup['usage'] > 0 else ""
                self.stdout.write(f"   🔸 {dup['name']} (ID: {dup['id']}){usage_info}")

            self.stdout.write('')

        if len(duplicates) > 20:
            self.stdout.write(f"... и еще {len(duplicates) - 20} групп")

        # Спрашиваем подтверждение
        if dry_run:
            self.stdout.write(self.style.WARNING('\n🏁 Сухой запуск - ничего не удалено'))
            return

        if not auto_confirm:
            confirm = input(f'\n❓ Удалить {total_duplicates} ключевых слов-дублей? (yes/no): ')
            if confirm.lower() != 'yes':
                self.stdout.write(self.style.WARNING('❌ Операция отменена'))
                return

        # Удаляем дубли
        deleted_count = self._delete_duplicates(duplicates, verbose)

        # Очищаем кэш Trie после удаления
        KeywordTrieManager().clear_cache()

        self.stdout.write(self.style.SUCCESS(f'\n✅ Удалено ключевых слов-дублей: {deleted_count}'))

        # Показываем итоговую статистику
        remaining = Keyword.objects.count()
        self.stdout.write(f"📊 Осталось ключевых слов в базе: {remaining}")
        self.stdout.write(f"📊 Удалено процентов: {(deleted_count / total_keywords * 100):.1f}%")

    def _find_duplicates(self, all_keywords, min_length, verbose):
        """Находит дубликаты ключевых слов"""
        from collections import defaultdict

        # Словарь для хранения найденных дублей
        duplicates = defaultdict(lambda: {'base_id': None, 'base_usage': 0, 'duplicates': []})

        # Создаем Trie для анализа форм
        trie_manager = KeywordTrieManager()
        trie = trie_manager.get_trie(verbose=False)

        # Группируем слова по возможным основам
        word_map = {kw.name.lower(): kw for kw in all_keywords}

        if verbose:
            self.stdout.write("🔎 Анализ слов...")

        processed = set()

        for keyword in tqdm(all_keywords, desc="Поиск дублей", disable=not verbose):
            word_lower = keyword.name.lower()

            # Пропускаем уже обработанные и слишком короткие слова
            if word_lower in processed or len(word_lower) < min_length:
                continue

            # Генерируем все возможные формы для этого слова
            all_forms = trie._generate_all_forms(word_lower)

            # Находим среди форм те, которые есть в базе как отдельные ключевые слова
            found_forms = []

            for form in all_forms:
                if form != word_lower and form in word_map:
                    other_kw = word_map[form]
                    # Проверяем, что это не одно и то же слово
                    if other_kw.id != keyword.id:
                        found_forms.append({
                            'id': other_kw.id,
                            'name': other_kw.name,
                            'usage': other_kw.cached_usage_count
                        })
                        processed.add(form)

            if found_forms:
                # Сортируем все слова в группе по длине (самое короткое - вероятно основа)
                all_in_group = [{
                    'id': keyword.id,
                    'name': keyword.name,
                    'usage': keyword.cached_usage_count,
                    'length': len(word_lower)
                }] + [{
                    'id': f['id'],
                    'name': f['name'],
                    'usage': f['usage'],
                    'length': len(f['name'])
                } for f in found_forms]

                # Сортируем по длине, затем по алфавиту
                all_in_group.sort(key=lambda x: (x['length'], x['name']))

                # Самое короткое слово считаем основой
                base = all_in_group[0]

                for item in all_in_group[1:]:
                    duplicates[base['name']]['base_id'] = base['id']
                    duplicates[base['name']]['base_usage'] = base['usage']
                    duplicates[base['name']]['duplicates'].append({
                        'id': item['id'],
                        'name': item['name'],
                        'usage': item['usage']
                    })

                processed.add(word_lower)

        return dict(duplicates)

    def _delete_duplicates(self, duplicates, verbose):
        """Удаляет найденные дубликаты"""
        deleted_count = 0

        for base_word, data in tqdm(duplicates.items(), desc="Удаление дублей", disable=not verbose):
            for dup in data['duplicates']:
                dup_id = dup['id']
                dup_name = dup['name']

                try:
                    # Получаем объект дубля и удаляем его
                    duplicate_kw = Keyword.objects.get(id=dup_id)
                    duplicate_kw.delete()
                    deleted_count += 1

                    if verbose:
                        self.stdout.write(self.style.SUCCESS(f"   ✅ Удален дубль: '{dup_name}' (ID: {dup_id})"))

                except Keyword.DoesNotExist:
                    self.stdout.write(self.style.ERROR(f"   ❌ Ошибка: ключевое слово не найдено (ID: {dup_id})"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"   ❌ Ошибка при удалении '{dup_name}': {e}"))

        return deleted_count