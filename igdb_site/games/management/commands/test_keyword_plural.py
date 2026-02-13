# games/management/commands/test_keyword_plural.py
from django.core.management.base import BaseCommand
from django.db import models
from games.models import Keyword, KeywordCategory
from games.analyze.keyword_trie import KeywordTrieManager
import time


class Command(BaseCommand):
    help = 'Test if keyword plural forms (army → armies) are correctly recognized'

    def add_arguments(self, parser):
        parser.add_argument(
            '--keyword',
            type=str,
            default='army',
            help='Base keyword to test (default: army)'
        )
        parser.add_argument(
            '--text',
            type=str,
            default='There are two armies in the game. Each army has its own commander.',
            help='Text to analyze'
        )
        parser.add_argument(
            '--list-all',
            action='store_true',
            help='List all keywords found in text, not just the test keyword'
        )

    def handle(self, *args, **options):
        keyword_name = options['keyword']
        test_text = options['text']
        list_all = options['list_all']

        self.stdout.write(self.style.SUCCESS(f'=== Testing keyword: "{keyword_name}" ==='))
        self.stdout.write(f'Test text: "{test_text}"')
        self.stdout.write('-' * 80)

        # Шаг 1: Создаем ключевое слово если его нет
        keyword, created = self._ensure_keyword_exists(keyword_name)
        if created:
            self.stdout.write(self.style.SUCCESS(f'✅ Created keyword: "{keyword_name}" (ID: {keyword.id})'))

        # Шаг 2: Тестируем через TrieManager (как в продакшене)
        self._test_trie_manager(keyword, test_text, list_all)

    def _ensure_keyword_exists(self, keyword_name):
        """Создаем ключевое слово если его нет"""
        keyword = Keyword.objects.filter(name__iexact=keyword_name).first()
        created = False

        if not keyword:
            # Создаем категорию если нет
            category, _ = KeywordCategory.objects.get_or_create(
                name='Test Category',
                defaults={'description': 'For testing purposes'}
            )

            # Создаем ключевое слово
            max_id = Keyword.objects.aggregate(models.Max('igdb_id'))['igdb_id__max'] or 1000000
            keyword = Keyword.objects.create(
                name=keyword_name,
                category=category,
                igdb_id=max_id + 1,
                cached_usage_count=0
            )
            created = True

        return keyword, created

    def _test_trie_manager(self, test_keyword, test_text, list_all):
        """Тестируем через TrieManager (продакшен)"""
        self.stdout.write(self.style.SUCCESS('\n🔍 Поиск ключевых слов в тексте:'))
        self.stdout.write('=' * 80)

        # Получаем Trie через менеджер
        start_time = time.time()
        trie_manager = KeywordTrieManager()
        trie = trie_manager.get_trie(verbose=False, force_rebuild=True)
        build_time = time.time() - start_time

        self.stdout.write(f'📊 Загружен Trie из базы данных за {build_time:.3f}с')

        # Ищем в тексте - ВСЕ вхождения, а не только уникальные
        start_time = time.time()
        # Изменено здесь: используем поиск всех вхождений
        results = trie.find_all_in_text(test_text, unique_only=False)
        search_time = time.time() - start_time

        self.stdout.write(f'🔎 Поиск завершен за {search_time:.4f}с')
        self.stdout.write(f'📈 Найдено {len(results)} совпадений ключевых слов')

        # Визуализируем текст с подсветкой
        self._visualize_text_with_keywords(test_text, results, trie)

        if list_all:
            self._print_all_results(results, trie, test_text)

        # Фильтруем результаты по нашему тестовому ключевому слову
        keyword_results = [r for r in results if r['id'] == test_keyword.id]

        if keyword_results:
            self.stdout.write(
                self.style.SUCCESS(f'\n✅ Ключевое слово "{test_keyword.name}" найдено {len(keyword_results)} раз:'))
            for i, result in enumerate(keyword_results, 1):
                kw_info = trie.get_keyword_by_id(result['id'])
                kw_name = kw_info['name'] if kw_info else f"ID:{result['id']}"

                # Показываем позицию в тексте с визуальным указателем
                self._show_position_in_text(test_text, result, i, trie)

                # Определяем форму
                form_type = self._get_form_type(result['text'], kw_name)

            self.stdout.write('\n' + '=' * 80)
        else:
            self.stdout.write(self.style.ERROR(f'\n❌ Ключевое слово "{test_keyword.name}" не найдено в тексте'))

        # Анализируем какие формы есть в Trie
        self._analyze_trie_forms(trie, test_keyword)

    def _visualize_text_with_keywords(self, text, results, trie):
        """Визуализируем текст с указанием позиций ключевых слов"""
        self.stdout.write('\n📝 ТЕКСТ С ОБНАРУЖЕННЫМИ КЛЮЧЕВЫМИ СЛОВАМИ:')
        self.stdout.write('-' * 80)

        # Создаем визуальную строку с маркерами
        markers = [' '] * len(text)

        for result in results:
            pos = result['position']
            length = result['length']
            kw_info = trie.get_keyword_by_id(result['id'])
            kw_name = kw_info['name'] if kw_info else 'Unknown'

            # Помечаем позицию в маркерах
            for i in range(pos, min(pos + length, len(text))):
                markers[i] = '^'

        # Выводим текст построчно с маркерами
        line_length = 70
        for i in range(0, len(text), line_length):
            text_chunk = text[i:i + line_length]
            marker_chunk = ''.join(markers[i:i + line_length])

            # Номер строки
            line_num = (i // line_length) + 1
            self.stdout.write(f'\nСтрока {line_num:2}:')

            # Текст
            self.stdout.write(f'       "{text_chunk}"')

            # Маркеры с пояснениями
            if '^' in marker_chunk:
                # Находим позиции ключевых слов в этой строке
                marker_line = ''
                for j, char in enumerate(marker_chunk):
                    if char == '^':
                        # Находим какое ключевое слово здесь
                        global_pos = i + j
                        for result in results:
                            if result['position'] <= global_pos < result['position'] + result['length']:
                                kw_info = trie.get_keyword_by_id(result['id'])
                                if kw_info:
                                    marker_line += f'↑{kw_info["name"]}'
                                else:
                                    marker_line += '↑KW'
                                break
                        else:
                            marker_line += '^'
                    else:
                        marker_line += ' '

                self.stdout.write(f'        {marker_line}')

        self.stdout.write('-' * 80)

    def _show_position_in_text(self, text, result, match_num, trie):
        """Показываем позицию в тексте с визуальным указателем"""
        kw_info = trie.get_keyword_by_id(result['id'])
        kw_name = kw_info['name'] if kw_info else f"ID:{result['id']}"

        pos = result['position']
        length = result['length']
        found_text = text[pos:pos + length]

        # Определяем контекст (около 40 символов вокруг)
        context_start = max(0, pos - 40)
        context_end = min(len(text), pos + length + 40)

        before_context = text[context_start:pos]
        after_context = text[pos + length:context_end]

        # Создаем строку с указателем позиции
        pointer_line = ' ' * len(before_context) + '^' * length

        self.stdout.write(f'\n🔹 Совпадение #{match_num}:')
        self.stdout.write(f'   Ключевое слово: "{kw_name}"')
        self.stdout.write(f'   Найдено как: "{found_text}"')
        self.stdout.write(f'   Позиция: символ {pos}')

        # Выводим контекст с указателем
        if context_start > 0:
            before_context = '...' + before_context
            pointer_line = '   ' + pointer_line
        else:
            pointer_line = '   ' + pointer_line

        self.stdout.write(f'\n   Контекст:')
        self.stdout.write(f'   "{before_context}{found_text}{after_context}"')
        if context_end < len(text):
            self.stdout.write('...')
        self.stdout.write(f'   {pointer_line}')

        # Определяем форму
        if found_text.lower().endswith('ies') and kw_name.lower().endswith('y'):
            self.stdout.write(
                self.style.SUCCESS(f'   → МНОЖЕСТВЕННАЯ ФОРМА: "{found_text}" распознана как "{kw_name}" (y→ies)'))
        elif found_text.lower().endswith('s') and not found_text.lower().endswith('ss'):
            self.stdout.write(
                self.style.SUCCESS(f'   → МНОЖЕСТВЕННАЯ ФОРМА: "{found_text}" распознана как "{kw_name}"'))
        else:
            self.stdout.write(f'   → БАЗОВАЯ ФОРМА: "{found_text}"')

    def _get_form_type(self, found_text, keyword_name):
        """Определяем тип формы"""
        found_lower = found_text.lower()
        keyword_lower = keyword_name.lower()

        if found_lower == keyword_lower:
            return "base"
        elif found_lower.endswith('ies') and keyword_lower.endswith('y'):
            return "plural_ies"
        elif found_lower.endswith('s') and not found_lower.endswith('ss'):
            return "plural_s"
        else:
            return "other"

    def _print_all_results(self, results, trie, test_text):
        """Выводим все найденные ключевые слова"""
        if not results:
            self.stdout.write('❌ Ключевые слова не найдены в тексте')
            return

        self.stdout.write('\n📋 ВСЕ НАЙДЕННЫЕ КЛЮЧЕВЫЕ СЛОВА:')
        self.stdout.write('=' * 80)

        # Группируем по ID ключевого слова
        grouped_results = {}
        for result in results:
            kw_id = result['id']
            if kw_id not in grouped_results:
                grouped_results[kw_id] = []
            grouped_results[kw_id].append(result)

        for kw_id, matches in grouped_results.items():
            kw_info = trie.get_keyword_by_id(kw_id)
            if kw_info:
                self.stdout.write(f'\n🔸 "{kw_info["name"]}" (ID: {kw_id}): {len(matches)} вхождение(ий)')

                for match_num, match in enumerate(matches[:5], 1):  # Показываем первые 5 вхождений
                    # Показываем позицию
                    pos = match['position']
                    found_text = test_text[pos:pos + match['length']]

                    # Контекст
                    context_start = max(0, pos - 30)
                    context_end = min(len(test_text), pos + match['length'] + 30)
                    context = test_text[context_start:context_end]
                    if context_start > 0:
                        context = '...' + context
                    if context_end < len(test_text):
                        context = context + '...'

                    # Определяем форму
                    form_type = self._get_form_type(found_text, kw_info['name'])
                    form_desc = {
                        "base": "базовая",
                        "plural_ies": "множ. (ies)",
                        "plural_s": "множ. (s)",
                        "other": "другая"
                    }.get(form_type, "неизвестная")

                    self.stdout.write(f'   {match_num}. Позиция {pos}: "{found_text}" ({form_desc})')
                    self.stdout.write(f'      Контекст: {context}')

                if len(matches) > 5:
                    self.stdout.write(f'   ... и еще {len(matches) - 5} вхождений')
            else:
                self.stdout.write(f'\n⚠️ Неизвестный ID ключевого слова: {kw_id}')

    def _analyze_trie_forms(self, trie, keyword):
        """Анализируем какие формы ключевого слова есть в Trie"""
        self.stdout.write(self.style.SUCCESS('\n🔧 АНАЛИЗ ФОРМ В TRIE:'))
        self.stdout.write('=' * 80)

        keyword_lower = keyword.name.lower()

        # Тестируем разные формы
        test_forms = [
            (keyword_lower, "базовая форма"),
            (keyword_lower + 's', "множ. число с 's'"),
            (keyword_lower[:-1] + 'ies', "множ. число y→ies"),
            (keyword_lower + 'es', "множ. число с 'es'"),
        ]

        found_forms = []

        self.stdout.write(f'\n🔍 Проверяем формы для ключевого слова "{keyword.name}":')

        for form, description in test_forms:
            # Проверяем, есть ли форма в Trie
            node = trie.root
            found = True
            for char in form:
                if char not in node.children:
                    found = False
                    break
                node = node.children[char]

            if found and node.is_end and node.keyword_id == keyword.id:
                found_forms.append((form, description))
                self.stdout.write(self.style.SUCCESS(f'   ✅ "{form}" ({description}) → "{keyword.name}"'))
            else:
                self.stdout.write(f'   ❌ "{form}" ({description}) — отсутствует')

        self.stdout.write(f'\n📊 ИТОГО: {len(found_forms)} форм из {len(test_forms)} доступно в Trie')

        # Показываем какие формы реально работают
        if found_forms:
            self.stdout.write(self.style.SUCCESS(f'\n✅ Доступные формы для поиска:'))
            for form, description in found_forms:
                self.stdout.write(f'   • {description}: "{form}"')