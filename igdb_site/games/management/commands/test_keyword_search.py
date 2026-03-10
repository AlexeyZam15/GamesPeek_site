# games/management/commands/test_keyword_search.py
"""
Тестовая команда для проверки поиска ключевых слов в тексте
Позволяет проверить, находит ли система ключевое слово в заданном тексте
"""

from django.core.management.base import BaseCommand
from games.analyze.keyword_trie import KeywordTrieManager
from games.models import Keyword
import time


class Command(BaseCommand):
    help = 'Тестирует поиск ключевого слова в тексте'

    def add_arguments(self, parser):
        parser.add_argument(
            '--text',
            type=str,
            required=True,
            help='Текст для анализа',
        )
        parser.add_argument(
            '--keyword',
            type=str,
            required=True,
            help='Ключевое слово для поиска',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод',
        )
        parser.add_argument(
            '--show-forms',
            action='store_true',
            help='Показать все формы ключевого слова',
        )
        parser.add_argument(
            '--no-rebuild',
            action='store_true',
            help='НЕ перестраивать Trie (использовать кэш)',
        )

    def handle(self, *args, **options):
        text = options['text']
        keyword_name = options['keyword']
        verbose = options['verbose']
        show_forms = options['show_forms']
        no_rebuild = options.get('no_rebuild', False)

        # ПО УМОЛЧАНИЮ ВСЕГДА ПЕРЕСТРАИВАЕМ, если не указано --no-rebuild
        force_rebuild = not no_rebuild

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("ТЕСТ ПОИСКА КЛЮЧЕВОГО СЛОВА"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📝 Текст: '{text}'")
        self.stdout.write(f"🔍 Ключевое слово: '{keyword_name}'")
        self.stdout.write(f"🔄 Перестройка Trie: {'ДА' if force_rebuild else 'НЕТ (используется кэш)'}")
        self.stdout.write("=" * 70)

        # Находим ключевое слово в базе
        try:
            keyword = Keyword.objects.get(name__iexact=keyword_name)
            self.stdout.write(f"✅ Ключевое слово найдено в БД: ID={keyword.id}, name='{keyword.name}'")
        except Keyword.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"❌ Ключевое слово '{keyword_name}' не найдено в БД"))
            return
        except Keyword.MultipleObjectsReturned:
            keywords = Keyword.objects.filter(name__iexact=keyword_name)
            self.stdout.write(self.style.WARNING(f"⚠️ Найдено несколько ключевых слов:"))
            for kw in keywords:
                self.stdout.write(f"   - ID={kw.id}, name='{kw.name}'")
            keyword = keywords.first()
            self.stdout.write(f"   Используем первое: ID={keyword.id}")

        # Получаем Trie с принудительной перестройкой
        self.stdout.write("\n🔨 Загружаем Trie ключевых слов...")
        start_time = time.time()

        trie_manager = KeywordTrieManager()
        trie = trie_manager.get_trie(verbose=verbose, force_rebuild=force_rebuild)

        load_time = time.time() - start_time
        self.stdout.write(f"✅ Trie загружен за {load_time:.3f} сек")

        # Показываем все формы ключевого слова если нужно
        if show_forms:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ФОРМЫ КЛЮЧЕВОГО СЛОВА"))
            self.stdout.write("=" * 70)

            # Получаем все формы из кэша или генерируем
            keyword_lower = keyword.name.lower()
            if keyword_lower in trie._all_forms_cache:
                forms = trie._all_forms_cache[keyword_lower]
            else:
                forms = trie._generate_all_forms(keyword_lower)

            self.stdout.write(f"📊 Всего форм: {len(forms)}")
            for form in sorted(forms):
                # Проверяем, есть ли форма в Trie
                node = trie.root
                found = True
                for char in form:
                    if char in node.children:
                        node = node.children[char]
                    else:
                        found = False
                        break

                status = "✅" if found and node.is_end else "❌"
                self.stdout.write(f"  {status} {form}")

            self.stdout.write("=" * 70)

        # Поиск в тексте
        self.stdout.write("\n🔍 Ищем ключевое слово в тексте...")
        start_time = time.time()

        # Используем find_all_in_text для поиска
        results = trie.find_all_in_text(text, unique_only=False)

        search_time = time.time() - start_time
        self.stdout.write(f"⏱️  Поиск выполнен за {search_time:.3f} сек")
        self.stdout.write(f"📊 Всего найдено совпадений: {len(results)}")

        # Фильтруем результаты для нашего ключевого слова
        keyword_results = [r for r in results if r['id'] == keyword.id]

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("РЕЗУЛЬТАТЫ ПОИСКА"))
        self.stdout.write("=" * 70)

        if keyword_results:
            self.stdout.write(self.style.SUCCESS(f"✅ Ключевое слово НАЙДЕНО!"))
            self.stdout.write(f"\nНайдено вхождений: {len(keyword_results)}")

            for i, result in enumerate(keyword_results, 1):
                self.stdout.write(f"\n--- Вхождение #{i} ---")
                self.stdout.write(f"  Позиция: {result['position']}")
                self.stdout.write(f"  Длина: {result['length']}")
                self.stdout.write(f"  Найденный текст: '{result['text']}'")

                # Показываем контекст
                start = max(0, result['position'] - 20)
                end = min(len(text), result['position'] + result['length'] + 20)
                context = text[start:end]

                # Подсвечиваем найденное слово в контексте
                rel_start = result['position'] - start
                rel_end = rel_start + result['length']
                highlighted = (
                        context[:rel_start] +
                        self.style.SUCCESS(context[rel_start:rel_end]) +
                        context[rel_end:]
                )
                self.stdout.write(f"  Контекст: ...{highlighted}...")

                # Дополнительная информация
                if result.get('is_phrase'):
                    self.stdout.write(f"  Тип: фраза из {len(result.get('words', []))} слов")
                if 'full_word' in result:
                    self.stdout.write(f"  В составе слова: '{result['full_word']}'")
        else:
            self.stdout.write(self.style.ERROR(f"❌ Ключевое слово НЕ НАЙДЕНО в тексте"))

            # Показываем что было найдено вместо этого
            if results:
                self.stdout.write("\nНайдены другие ключевые слова:")
                other_ids = set(r['id'] for r in results)
                for kw_id in other_ids:
                    kw_data = trie.keywords_cache.get(kw_id)
                    if kw_data:
                        self.stdout.write(f"  • {kw_data['name']} (ID: {kw_id})")

        self.stdout.write("\n" + "=" * 70)

        # Проверяем, есть ли ключевое слово в Trie
        self.stdout.write("\n🔍 ПРОВЕРКА НАЛИЧИЯ В TRIE:")
        keyword_lower = keyword.name.lower()
        node = trie.root
        found_in_trie = True
        path = []

        for char in keyword_lower:
            if char in node.children:
                node = node.children[char]
                path.append(char)
            else:
                found_in_trie = False
                break

        if found_in_trie and node.is_end:
            self.stdout.write(self.style.SUCCESS(f"  ✅ Ключевое слово '{keyword_lower}' есть в Trie!"))
            self.stdout.write(f"     Указывает на: {node.keyword_name} (ID: {node.keyword_id})")
        else:
            self.stdout.write(self.style.ERROR(f"  ❌ Ключевое слово '{keyword_lower}' ОТСУТСТВУЕТ в Trie"))
            if path:
                self.stdout.write(f"     Найдена часть: '{''.join(path)}'")

        self.stdout.write("\n" + "=" * 70)