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
        parser.add_argument(
            '--text',
            type=str,
            required=True,
            help='Текст для анализа',
        )
        parser.add_argument(
            '--keywords',
            type=str,
            help='Ключевые слова для поиска (через запятую). Если не указаны, ищутся все ключевые слова из БД',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод',
        )
        parser.add_argument(
            '--show-forms',
            action='store_true',
            help='Показать все формы ключевых слов',
        )
        parser.add_argument(
            '--no-rebuild',
            action='store_true',
            help='НЕ перестраивать Trie (использовать кэш)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Максимальное количество ключевых слов для показа (по умолчанию 50)',
        )

    def handle(self, *args, **options):
        text = options['text']
        keywords_input = options.get('keywords')
        verbose = options['verbose']
        show_forms = options['show_forms']
        no_rebuild = options.get('no_rebuild', False)
        limit = options.get('limit', 50)

        force_rebuild = not no_rebuild

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("ТЕСТ ПОИСКА КЛЮЧЕВЫХ СЛОВ"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📝 Текст: '{text}'")

        if keywords_input:
            keyword_names = [k.strip() for k in keywords_input.split(',')]
            self.stdout.write(f"🔍 Ключевые слова: {', '.join(keyword_names)} (из списка)")
        else:
            keyword_names = None
            self.stdout.write(f"🔍 Режим: поиск ВСЕХ ключевых слов из БД")

        self.stdout.write(f"🔄 Перестройка Trie: {'ДА' if force_rebuild else 'НЕТ (используется кэш)'}")
        self.stdout.write(f"📊 Лимит показа: {limit} ключевых слов")
        self.stdout.write("=" * 70)

        # Получаем Trie с принудительной перестройкой
        self.stdout.write("\n🔨 Загружаем Trie ключевых слов...")
        start_time = time.time()

        trie_manager = KeywordTrieManager()
        trie = trie_manager.get_trie(verbose=verbose, force_rebuild=force_rebuild)

        load_time = time.time() - start_time
        self.stdout.write(f"✅ Trie загружен за {load_time:.3f} сек")
        self.stdout.write(f"📊 Всего ключевых слов в БД: {Keyword.objects.count()}")

        # Находим ключевые слова для поиска
        keywords_to_check = []
        not_found = []

        if keyword_names:
            # Режим с указанными ключевыми словами
            for kw_name in keyword_names:
                try:
                    keyword = Keyword.objects.get(name__iexact=kw_name)
                    self.stdout.write(f"✅ Ключевое слово найдено в БД: ID={keyword.id}, name='{keyword.name}'")
                    keywords_to_check.append(keyword)
                except Keyword.DoesNotExist:
                    self.stdout.write(self.style.ERROR(f"❌ Ключевое слово '{kw_name}' не найдено в БД"))
                    not_found.append(kw_name)
                except Keyword.MultipleObjectsReturned:
                    kw_list = Keyword.objects.filter(name__iexact=kw_name)
                    self.stdout.write(self.style.WARNING(f"⚠️ Найдено несколько ключевых слов для '{kw_name}':"))
                    for kw in kw_list:
                        self.stdout.write(f"   - ID={kw.id}, name='{kw.name}'")
                    keyword = kw_list.first()
                    self.stdout.write(f"   Используем первое: ID={keyword.id}")
                    keywords_to_check.append(keyword)

            if not_found:
                self.stdout.write(self.style.WARNING(f"\n⚠️ Не найдены ключевые слова: {', '.join(not_found)}"))

            if not keywords_to_check:
                self.stdout.write(self.style.ERROR("❌ Нет ключевых слов для поиска"))
                return
        else:
            # Режим с поиском всех ключевых слов
            self.stdout.write("\n📋 Загружаем все ключевые слова из БД...")
            keywords_to_check = list(Keyword.objects.all().order_by('name'))
            self.stdout.write(f"✅ Загружено {len(keywords_to_check)} ключевых слов")

        # Показываем все формы ключевых слов если нужно
        if show_forms and keywords_to_check:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ФОРМЫ КЛЮЧЕВЫХ СЛОВ"))
            self.stdout.write("=" * 70)

            # Показываем только первые limit форм если их много
            show_limit = min(limit, len(keywords_to_check))
            for keyword in keywords_to_check[:show_limit]:
                self.stdout.write(f"\n📌 {keyword.name}:")

                # Получаем все формы из кэша или генерируем
                keyword_lower = keyword.name.lower()
                if keyword_lower in trie._all_forms_cache:
                    forms = trie._all_forms_cache[keyword_lower]
                else:
                    forms = trie._generate_all_forms(keyword_lower)

                self.stdout.write(f"   Всего форм: {len(forms)}")
                for form in sorted(forms)[:10]:  # Показываем первые 10 форм
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
                    self.stdout.write(f"     {status} {form}")

                if len(forms) > 10:
                    self.stdout.write(f"     ... и еще {len(forms) - 10} форм")

            if len(keywords_to_check) > show_limit:
                self.stdout.write(f"\n   ... и еще {len(keywords_to_check) - show_limit} ключевых слов")

        # Поиск в тексте
        self.stdout.write("\n🔍 Ищем ключевые слова в тексте...")
        start_time = time.time()

        # Используем find_all_in_text для поиска
        results = trie.find_all_in_text(text, unique_only=False)

        search_time = time.time() - start_time
        self.stdout.write(f"⏱️  Поиск выполнен за {search_time:.3f} сек")
        self.stdout.write(f"📊 Всего найдено совпадений: {len(results)}")

        # Группируем результаты по ключевым словам
        keyword_results = defaultdict(list)
        for r in results:
            keyword_results[r['id']].append(r)

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("РЕЗУЛЬТАТЫ ПОИСКА"))
        self.stdout.write("=" * 70)

        if keyword_names:
            # Режим с указанными ключевыми словами
            for keyword in keywords_to_check:
                self.stdout.write(f"\n🔸 {keyword.name}:")

                if keyword.id in keyword_results:
                    matches = keyword_results[keyword.id]
                    self.stdout.write(self.style.SUCCESS(f"   ✅ НАЙДЕНО! ({len(matches)} вхождений)"))

                    for i, result in enumerate(matches, 1):
                        self.stdout.write(f"\n   --- Вхождение #{i} ---")
                        self.stdout.write(f"     Позиция: {result['position']}")
                        self.stdout.write(f"     Найденный текст: '{result['text']}'")

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
                        self.stdout.write(f"     Контекст: ...{highlighted}...")

                        # Дополнительная информация
                        if result.get('is_phrase'):
                            self.stdout.write(f"     Тип: фраза")
                        if 'full_word' in result:
                            self.stdout.write(f"     В составе слова: '{result['full_word']}'")
                else:
                    self.stdout.write(self.style.ERROR(f"   ❌ НЕ НАЙДЕНО"))
        else:
            # Режим со всеми ключевыми словами
            found_keywords = []
            for keyword in keywords_to_check:
                if keyword.id in keyword_results:
                    matches = keyword_results[keyword.id]
                    found_keywords.append((keyword, len(matches)))

            # Сортируем по количеству вхождений (сначала самые частые)
            found_keywords.sort(key=lambda x: x[1], reverse=True)

            self.stdout.write(f"\n📊 Найдено {len(found_keywords)} ключевых слов из {len(keywords_to_check)}")
            self.stdout.write(f"\n📋 ТОП-{min(limit, len(found_keywords))} НАЙДЕННЫХ КЛЮЧЕВЫХ СЛОВ:")

            for i, (keyword, count) in enumerate(found_keywords[:limit], 1):
                matches = keyword_results[keyword.id]
                first_match = matches[0]

                # Показываем первое вхождение
                start = max(0, first_match['position'] - 20)
                end = min(len(text), first_match['position'] + first_match['length'] + 20)
                context = text[start:end]

                rel_start = first_match['position'] - start
                rel_end = rel_start + first_match['length']
                highlighted = (
                        context[:rel_start] +
                        self.style.SUCCESS(context[rel_start:rel_end]) +
                        context[rel_end:]
                )

                self.stdout.write(f"\n  {i}. {keyword.name} (ID: {keyword.id}) - {count} вхожд.")
                self.stdout.write(f"     Контекст: ...{highlighted}...")

            if len(found_keywords) > limit:
                self.stdout.write(f"\n   ... и еще {len(found_keywords) - limit} ключевых слов")

        # Показываем другие найденные ключевые слова (только в verbose режиме)
        if verbose and keyword_names:
            other_ids = set(keyword_results.keys()) - {k.id for k in keywords_to_check}
            if other_ids:
                self.stdout.write("\n📌 ДРУГИЕ НАЙДЕННЫЕ КЛЮЧЕВЫЕ СЛОВА:")
                for kw_id in list(other_ids)[:limit]:
                    kw_data = trie.keywords_cache.get(kw_id)
                    if kw_data:
                        matches = keyword_results[kw_id]
                        self.stdout.write(f"   • {kw_data['name']} (ID: {kw_id}) - {len(matches)} вхожд.")

        # Проверка конфликтов (пересечений)
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("ПРОВЕРКА КОНФЛИКТОВ"))
        self.stdout.write("=" * 70)

        # Сортируем результаты по позиции
        all_results = sorted(results, key=lambda x: x['position'])

        conflicts = []
        for i in range(len(all_results)):
            for j in range(i + 1, len(all_results)):
                r1 = all_results[i]
                r2 = all_results[j]

                # Проверяем пересечение
                r1_end = r1['position'] + r1['length']
                r2_end = r2['position'] + r2['length']

                if r1['position'] < r2_end and r2['position'] < r1_end:
                    # Нашли пересечение
                    kw1 = trie.keywords_cache.get(r1['id'], {}).get('name', f'ID:{r1["id"]}')
                    kw2 = trie.keywords_cache.get(r2['id'], {}).get('name', f'ID:{r2["id"]}')
                    conflicts.append((r1, r2, kw1, kw2))

        if conflicts:
            self.stdout.write(self.style.WARNING(f"⚠️ Найдено {len(conflicts)} пересечений:"))
            for r1, r2, kw1, kw2 in conflicts[:limit]:  # Ограничиваем вывод
                self.stdout.write(f"\n   • {kw1} (поз.{r1['position']}-{r1['position'] + r1['length']})")
                self.stdout.write(f"     {kw2} (поз.{r2['position']}-{r2['position'] + r2['length']})")
                self.stdout.write(
                    f"     Текст: '{text[max(0, r1['position'] - 10):min(len(text), r2['position'] + r2['length'] + 10)]}'")

            if len(conflicts) > limit:
                self.stdout.write(f"\n   ... и еще {len(conflicts) - limit} пересечений")
        else:
            self.stdout.write(self.style.SUCCESS("✅ Конфликтов не найдено"))

        # Проверка наличия ключевых слов в Trie (только для указанных)
        if keyword_names:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ПРОВЕРКА НАЛИЧИЯ В TRIE"))
            self.stdout.write("=" * 70)

            for keyword in keywords_to_check:
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
                    self.stdout.write(self.style.SUCCESS(f"  ✅ '{keyword.name}' есть в Trie! (ID: {node.keyword_id})"))
                else:
                    self.stdout.write(self.style.ERROR(f"  ❌ '{keyword.name}' ОТСУТСТВУЕТ в Trie"))
                    if path:
                        self.stdout.write(f"     Найдена часть: '{''.join(path)}'")

        self.stdout.write("\n" + "=" * 70)