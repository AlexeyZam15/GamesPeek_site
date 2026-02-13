# games/management/commands/test_keyword_verb_forms.py
from django.core.management.base import BaseCommand
from games.analyze.keyword_trie import KeywordTrieManager


class Command(BaseCommand):
    help = 'Test if keyword verb forms (divide → divided, destroying, etc.) are correctly recognized'

    def add_arguments(self, parser):
        parser.add_argument(
            '--keyword',
            type=str,
            default='divide',
            help='Base keyword to test (default: divide)'
        )
        parser.add_argument(
            '--text',
            type=str,
            default='The game takes place on an island continent called Riberia, which had long been divided and ruled under four kingdoms.',
            help='Text to analyze'
        )

    def handle(self, *args, **options):
        keyword_name = options['keyword'].lower()
        test_text = options['text']

        self.stdout.write('=' * 80)
        self.stdout.write(f'🔍 ТЕСТ: "{keyword_name}" → глагольные формы')
        self.stdout.write('=' * 80)
        self.stdout.write(f'📝 Текст: "{test_text}"')
        self.stdout.write('-' * 80)

        # Получаем Trie (автоматически перестроится если нужно)
        trie_manager = KeywordTrieManager()
        trie = trie_manager.get_trie(verbose=True)

        # Ищем ключевое слово в тексте
        self.stdout.write(f'\n🔎 Ищем формы "{keyword_name}" в тексте:')

        results = trie.find_all_in_text(test_text, unique_only=False)

        # Фильтруем результаты по нашему ключевому слову
        keyword_results = []
        for result in results:
            kw_info = trie.get_keyword_by_id(result['id'])
            if kw_info and kw_info['name'].lower() == keyword_name:
                keyword_results.append(result)

        if keyword_results:
            self.stdout.write(f'✅ Найдено {len(keyword_results)} форм:')
            for i, result in enumerate(keyword_results, 1):
                pos = result['position']
                found_text = result['text']

                # Показываем контекст
                context_start = max(0, pos - 30)
                context_end = min(len(test_text), pos + len(found_text) + 30)
                context = test_text[context_start:context_end]

                if context_start > 0:
                    context = '...' + context
                if context_end < len(test_text):
                    context = context + '...'

                self.stdout.write(f'\n  {i}. "{found_text}" (позиция: {pos})')
                self.stdout.write(f'     Контекст: {context}')

                # Определяем тип формы
                form_type = self._get_form_type(found_text, keyword_name)
                self.stdout.write(f'     Тип: {form_type}')
        else:
            self.stdout.write(f'❌ Формы "{keyword_name}" не найдены')

            # Показываем отладочную информацию
            self.stdout.write(f'\n🔧 Отладка:')

            # Проверяем, есть ли слово в Trie вообще
            found_in_trie = False
            for kw_id, kw_data in trie.keywords_cache.items():
                if kw_data['name'].lower() == keyword_name:
                    found_in_trie = True
                    self.stdout.write(f'✅ "{keyword_name}" есть в базе (ID: {kw_id})')

                    # Проверяем основные формы
                    test_forms = [keyword_name,
                                  keyword_name + 'd' if keyword_name.endswith('e') else keyword_name + 'ed',
                                  keyword_name[:-1] + 'ing' if keyword_name.endswith('e') else keyword_name + 'ing',
                                  keyword_name + 's']

                    for form in test_forms:
                        node = trie.root
                        has_form = True
                        for char in form:
                            if char not in node.children:
                                has_form = False
                                break
                            node = node.children[char]

                        if has_form and node.is_end and node.keyword_id == kw_id:
                            self.stdout.write(f'✅ Форма "{form}" есть в Trie')
                        else:
                            self.stdout.write(f'❌ Форма "{form}" отсутствует в Trie')
                    break

            if not found_in_trie:
                self.stdout.write(f'❌ "{keyword_name}" отсутствует в базе ключевых слов')

    def _get_form_type(self, found_text, keyword_name):
        """Определяем тип формы"""
        if found_text == keyword_name:
            return "базовая форма"
        elif found_text.endswith('ing'):
            return "глагольная форма -ing"
        elif found_text.endswith('ed'):
            return "прошедшее время -ed"
        elif found_text.endswith('d') and keyword_name.endswith('e'):
            return "прошедшее время -d"
        elif found_text.endswith('s'):
            return "3-е лицо ед.ч."
        else:
            return "другая форма"