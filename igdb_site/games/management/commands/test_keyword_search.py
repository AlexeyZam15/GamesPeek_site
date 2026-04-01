# games/management/commands/test_keyword_search.py
"""
Тестовая команда для проверки поиска ключевых слов в тексте
Позволяет проверить, находит ли система ключевые слова в заданном тексте
"""

from django.core.management.base import BaseCommand
from games.analyze.keyword_trie import KeywordTrieManager
from games.models import Keyword
import time
from collections import defaultdict


class Command(BaseCommand):
    help = 'Тестирует поиск ключевых слов в тексте'

    def add_arguments(self, parser):
        parser.add_argument('--text', type=str, required=True, help='Текст для анализа')
        parser.add_argument('--keywords', type=str, help='Ключевые слова для поиска (через запятую)')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод')
        parser.add_argument('--no-rebuild', action='store_true', help='НЕ перестраивать Trie')
        parser.add_argument('--limit', type=int, default=50, help='Лимит показа')

    def _print_header(self, title):
        """Выводит заголовок секции"""
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS(title))
        self.stdout.write("=" * 60)

    def _print_params(self, text, keywords_input, no_rebuild, limit):
        """Выводит параметры запуска"""
        self._print_header("ТЕСТ ПОИСКА КЛЮЧЕВЫХ СЛОВ")
        self.stdout.write(f"📝 Текст: '{text}'")
        self.stdout.write(f"🔍 Ищем: {keywords_input or 'ВСЕ ключевые слова'}")
        self.stdout.write(f"🔄 Перестройка Trie: {'НЕТ' if no_rebuild else 'ДА'}")
        self.stdout.write(f"📊 Лимит: {limit}")

    def _load_trie(self, verbose, no_rebuild):
        """Загружает Trie и возвращает его"""
        self.stdout.write("\n🔨 Загрузка Trie...")
        start_time = time.time()
        trie = KeywordTrieManager().get_trie(verbose=verbose, force_rebuild=not no_rebuild)
        load_time = time.time() - start_time
        self.stdout.write(f"   ✅ Загружено за {load_time:.2f}с | {Keyword.objects.count()} ключевых слов в БД")

        # Проверка WordNet
        if not trie._ensure_wordnet().is_available():
            self.stdout.write(self.style.ERROR("\n❌ WordNetAPI недоступен"))
            return None

        return trie

    def _prepare_keywords(self, keywords_input):
        """Подготавливает список ключевых слов для проверки"""
        keywords_to_check = []

        if keywords_input:
            keyword_names = [k.strip() for k in keywords_input.split(',')]
            self.stdout.write("\n🔎 Проверяемые ключевые слова:")

            for kw_name in keyword_names:
                try:
                    kw = Keyword.objects.get(name__iexact=kw_name)
                    keywords_to_check.append(kw)
                    self.stdout.write(f"   ✅ {kw_name} (ID:{kw.id})")
                except Keyword.DoesNotExist:
                    self.stdout.write(f"   ❌ {kw_name} (не найдено в БД)")

            if not keywords_to_check:
                self.stdout.write(self.style.ERROR("❌ Нет ключевых слов для поиска"))
                return None
        else:
            keywords_to_check = list(Keyword.objects.all().order_by('name'))
            self.stdout.write(f"\n📋 Загружено {len(keywords_to_check)} ключевых слов из БД")

        return keywords_to_check

    def _search_in_text(self, trie, text, verbose):
        """Выполняет поиск в тексте и возвращает результаты"""
        self._print_header(f"РЕЗУЛЬТАТЫ ПОИСКА В ТЕКСТЕ: '{text}'")
        search_start = time.time()

        try:
            # Включаем verbose в самом Trie если нужно
            if verbose:
                original_verbose = trie.verbose
                trie.verbose = True

            results = trie.find_all_in_text(text, unique_only=False)

            # Восстанавливаем verbose
            if verbose:
                trie.verbose = original_verbose

        except RuntimeError as e:
            self.stdout.write(self.style.ERROR(f"❌ {e}"))
            return None, 0

        search_time = time.time() - search_start
        self.stdout.write(f"⏱️  Поиск: {search_time:.2f}с | Найдено совпадений: {len(results)}")
        self.stdout.write("")

        return results, search_time

    def _group_results(self, results):
        """Группирует результаты по ID ключевых слов"""
        keyword_results = defaultdict(list)
        for r in results:
            keyword_results[r['id']].append(r)
        return keyword_results

    def _display_results_for_specific(self, keywords_to_check, keyword_results, text, verbose):
        """Отображает результаты для указанных ключевых слов"""
        for kw in keywords_to_check:
            matches = keyword_results.get(kw.id, [])
            if matches:
                self.stdout.write(self.style.SUCCESS(f"✅ {kw.name}"))

                # Показываем все найденные формы
                for match in matches:
                    start = max(0, match['position'] - 20)
                    end = min(len(text), match['position'] + match['length'] + 20)
                    context = text[start:end]
                    rel_start = match['position'] - start
                    rel_end = rel_start + match['length']

                    self.stdout.write(
                        f"   → найдено: '{match['text']}' (поз.{match['position']}-{match['position'] + match['length']})")
                    self.stdout.write(f"   → лемма: {match.get('matched_lemma', '?')}")
                    self.stdout.write(
                        f"   ...{context[:rel_start]}[{context[rel_start:rel_end]}]{context[rel_end:]}...")
            else:
                self.stdout.write(self.style.ERROR(f"❌ {kw.name}"))
                self.stdout.write(f"   → не найдено")

    def _display_results_for_all(self, keywords_to_check, keyword_results, text, verbose, limit):
        """Отображает результаты для всех ключевых слов"""
        found = [(kw, len(keyword_results.get(kw.id, []))) for kw in keywords_to_check]
        found = [(kw, cnt) for kw, cnt in found if cnt > 0]
        found.sort(key=lambda x: x[1], reverse=True)

        self.stdout.write(f"\n📊 Найдено {len(found)} из {len(keywords_to_check)} ключевых слов")

        for kw, cnt in found[:limit]:
            matches = keyword_results[kw.id]
            forms = set(m['text'] for m in matches)
            self.stdout.write(f"\n  {cnt:2d} × {kw.name}")
            self.stdout.write(f"     формы: {', '.join(sorted(forms)[:3])}")

            if verbose:
                first = matches[0]
                self.stdout.write(f"     пример: '{first['text']}' → '{first['matched_lemma']}'")
                start = max(0, first['position'] - 20)
                end = min(len(text), first['position'] + first['length'] + 20)
                context = text[start:end]
                rel_start = first['position'] - start
                rel_end = rel_start + first['length']
                self.stdout.write(f"     ...{context[:rel_start]}[{context[rel_start:rel_end]}]{context[rel_end:]}...")

        return found

    def _check_trie_presence(self, keywords_to_check, trie):
        """Проверяет наличие ключевых слов в Trie"""
        self._print_header("ПРОВЕРКА TRIE")
        for kw in keywords_to_check:
            node = trie.root
            found_path = True
            for ch in kw.name.lower():
                if ch in node.children:
                    node = node.children[ch]
                else:
                    found_path = False
                    break

            if found_path and node.is_end:
                self.stdout.write(f"✅ {kw.name} (ID:{node.keyword_id}) - есть в Trie")
            else:
                self.stdout.write(f"❌ {kw.name} - отсутствует в Trie")

    def _print_summary(self, keywords_to_check, keyword_results, text, keywords_input, found=None):
        """Выводит итоговую статистику"""
        self._print_header("ИТОГ")

        if keywords_input:
            found_count = sum(1 for kw in keywords_to_check if kw.id in keyword_results)
        else:
            found_count = len(found) if found else 0

        self.stdout.write(f"📊 Найдено {found_count} из {len(keywords_to_check)} ключевых слов")
        self.stdout.write(f"📝 Анализировался текст: '{text}'")

        if keywords_input:
            for kw in keywords_to_check:
                matches = keyword_results.get(kw.id, [])
                if matches:
                    self.stdout.write(f"   ✅ {kw.name} ({len(matches)} вхожд.)")
                else:
                    self.stdout.write(f"   ❌ {kw.name} (0 вхожд.)")

        self.stdout.write("=" * 60)

    def handle(self, *args, **options):
        text = options['text']
        keywords_input = options.get('keywords')
        verbose = options['verbose']
        no_rebuild = options.get('no_rebuild', False)
        limit = options.get('limit', 50)

        # 1. Параметры запуска
        self._print_params(text, keywords_input, no_rebuild, limit)

        # 2. Загрузка Trie
        trie = self._load_trie(verbose, no_rebuild)
        if not trie:
            return

        # 3. Подготовка ключевых слов
        keywords_to_check = self._prepare_keywords(keywords_input)
        if not keywords_to_check:
            return

        # 4. Поиск в тексте
        results, search_time = self._search_in_text(trie, text, verbose)
        if results is None:
            return

        # 5. Группировка результатов
        keyword_results = self._group_results(results)

        # 6. Отображение результатов
        if keywords_input:
            self._display_results_for_specific(keywords_to_check, keyword_results, text, verbose)
        else:
            found = self._display_results_for_all(keywords_to_check, keyword_results, text, verbose, limit)

        # 7. Проверка наличия в Trie
        if keywords_input:
            self._check_trie_presence(keywords_to_check, trie)

        # 8. Итог
        if keywords_input:
            self._print_summary(keywords_to_check, keyword_results, text, keywords_input)
        else:
            self._print_summary(keywords_to_check, keyword_results, text, keywords_input, found)