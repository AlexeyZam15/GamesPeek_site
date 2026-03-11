# games/management/commands/test_normalization.py
"""
Django команда для тестирования нормализации ключевых слов:
- Тестирует отдельные слова
- Тестирует группы слов
- Показывает статистику нормализации
- Сравнивает разные методы нормализации
"""

from django.core.management.base import BaseCommand
from games.models import Keyword
from games.management.commands.normalize_keywords import Command as NormalizeCommand
from collections import defaultdict
import time


class Command(BaseCommand):
    help = 'Тестирует нормализацию ключевых слов'

    def add_arguments(self, parser):
        parser.add_argument(
            '--words',
            type=str,
            nargs='+',
            help='Список слов для тестирования (например: stretching drawing running)',
        )
        parser.add_argument(
            '--test-all',
            action='store_true',
            help='Тестировать все ключевые слова из базы',
        )
        parser.add_argument(
            '--check-group',
            type=str,
            help='Проверить группу для конкретного слова (например: stretching)',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод',
        )
        parser.add_argument(
            '--compare',
            action='store_true',
            help='Сравнить разные методы нормализации',
        )

    def _init_normalizer(self):
        """Инициализирует нормализатор из основной команды"""
        self.normalizer = NormalizeCommand()
        self.normalizer._init_nltk()

        # Кэш для WordNet, чтобы не обращаться постоянно
        try:
            from nltk.corpus import wordnet as wn
            self.wn = wn
        except:
            self.wn = None

    def _test_single_word(self, word: str, verbose: bool = False) -> dict:
        """
        Тестирует нормализацию одного слова всеми методами
        Возвращает словарь с результатами
        """
        results = {
            'original': word,
            'lowercase': word.lower(),
            'in_wordnet': False,
            'is_gaming_term': False,
            'is_short_word': False,
            'normalization_steps': [],
            'base_form_combined': None,
            'exists_in_db': False,
            'db_object': None,
        }

        word_lower = word.lower()

        # Проверяем игровые термины
        results['is_gaming_term'] = self.normalizer._is_gaming_term(word)
        if results['is_gaming_term']:
            results['normalization_steps'].append("🎮 Игровой термин - не нормализуем")

        # Проверяем короткие слова
        results['is_short_word'] = self.normalizer._is_short_word(word)
        if results['is_short_word']:
            results['normalization_steps'].append("📏 Короткое слово - не нормализуем")

        # Нормализация
        if not results['is_gaming_term'] and not results['is_short_word']:
            # Показываем шаги нормализации
            if word_lower.endswith('ing'):
                base = word_lower[:-3]
                results['normalization_steps'].append(f"1. Убираем 'ing' → '{base}'")

                if len(base) >= 2 and base[-1] == base[-2]:
                    results['normalization_steps'].append(f"2. Убираем удвоенную согласную → '{base[:-1]}'")

                if len(base) >= 2 and base[-1] not in 'aeiou' and not base.endswith('e'):
                    results['normalization_steps'].append(f"3. Пробуем добавить 'e' → '{base + 'e'}'")

        results['base_form_combined'] = self.normalizer._get_base_form(word)

        # Проверяем в WordNet (уже после нормализации)
        if self.wn:
            results['in_wordnet'] = bool(self.wn.synsets(results['base_form_combined']))
            if results['in_wordnet']:
                results['normalization_steps'].append(f"✅ Форма '{results['base_form_combined']}' найдена в WordNet")

        # Проверяем в базе данных
        try:
            kw = Keyword.objects.filter(name__iexact=word).first()
            if kw:
                results['exists_in_db'] = True
                results['db_object'] = {
                    'id': kw.id,
                    'name': kw.name,
                    'games_count': kw.game_set.count()
                }
        except:
            pass

        return results

    def _print_word_result(self, result: dict, verbose: bool = False):
        """
        Выводит результат тестирования одного слова
        """
        original = result['original']

        # Определяем статус
        if result['base_form_combined'] != original.lower():
            status = self.style.SUCCESS("🔨 НОРМАЛИЗУЕТСЯ")
            arrow = " → "
        else:
            status = self.style.WARNING("⏺️ НЕ ИЗМЕНЯЕТСЯ")
            arrow = " = "

        self.stdout.write(f"\n{status} {original}{arrow}{result['base_form_combined']}")

        if verbose:
            self.stdout.write(f"   📊 Статистика:")
            self.stdout.write(f"      • В нижнем регистре: {result['lowercase']}")
            self.stdout.write(f"      • В WordNet: {'✅ да' if result['in_wordnet'] else '❌ нет'}")
            self.stdout.write(f"      • Игровой термин: {'✅ да' if result['is_gaming_term'] else '❌ нет'}")
            self.stdout.write(f"      • Короткое слово: {'✅ да' if result['is_short_word'] else '❌ нет'}")
            self.stdout.write(f"      • По правилам: {result['base_form_rules']}")

            if result['exists_in_db']:
                db = result['db_object']
                self.stdout.write(f"      • В базе: ✅ ID:{db['id']} '{db['name']}' (игр: {db['games_count']})")
            else:
                self.stdout.write(f"      • В базе: ❌ нет")

        # Показываем грамматические формы если есть
        if result['base_form_combined'] != original.lower():
            self.stdout.write(f"   🔍 Грамматическая форма: '{original}' → '{result['base_form_combined']}'")

    def _find_word_group(self, word: str) -> dict:
        """
        Находит все слова в базе, которые могут быть формами данного слова
        """
        from django.db.models import Q

        word_lower = word.lower()
        base_form = self.normalizer._get_base_form(word_lower)

        result = {
            'search_word': word,
            'base_form': base_form,
            'exact_matches': [],
            'possible_forms': [],
            'all_related': [],
        }

        # Точное совпадение
        exact = Keyword.objects.filter(name__iexact=word_lower).first()
        if exact:
            result['exact_matches'].append({
                'id': exact.id,
                'name': exact.name,
                'games_count': exact.game_set.count()
            })

        # Ищем возможные формы
        all_keywords = Keyword.objects.all()

        for kw in all_keywords:
            kw_lower = kw.name.lower()
            kw_base = self.normalizer._get_base_form(kw_lower)

            if kw_base == base_form and kw_lower != base_form:
                result['possible_forms'].append({
                    'id': kw.id,
                    'name': kw.name,
                    'games_count': kw.game_set.count(),
                    'base': kw_base
                })

            if kw_lower == base_form and kw_lower != word_lower:
                result['all_related'].append({
                    'id': kw.id,
                    'name': kw.name,
                    'games_count': kw.game_set.count(),
                    'relation': 'базовая форма'
                })

        return result

    def _print_word_group(self, group: dict):
        """
        Выводит информацию о группе слов
        """
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS(f"ГРУППА ДЛЯ СЛОВА: {group['search_word']}"))
        self.stdout.write("=" * 70)

        self.stdout.write(f"\n📌 Базовая форма: {group['base_form']}")

        if group['exact_matches']:
            match = group['exact_matches'][0]
            self.stdout.write(f"\n✅ Точное совпадение в базе:")
            self.stdout.write(f"   • {match['name']} (ID: {match['id']}) - игр: {match['games_count']}")

        if group['possible_forms']:
            self.stdout.write(f"\n🔀 Возможные формы (все нормализуются к '{group['base_form']}'):")
            for form in sorted(group['possible_forms'], key=lambda x: x['games_count'], reverse=True):
                self.stdout.write(f"   • {form['name']} (ID: {form['id']}) - игр: {form['games_count']}")

        if group['all_related']:
            self.stdout.write(f"\n🔗 Связанные слова:")
            for rel in group['all_related']:
                self.stdout.write(
                    f"   • {rel['name']} (ID: {rel['id']}) - {rel['relation']}, игр: {rel['games_count']}")

        # Подсчитываем общую статистику
        total_forms = len(group['possible_forms'])
        total_games = sum(f['games_count'] for f in group['possible_forms'])

        if group['exact_matches']:
            total_games += group['exact_matches'][0]['games_count']

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(f"📊 Итого: {total_forms} форм, {total_games} игр")
        self.stdout.write("=" * 70)

    def _test_all_keywords(self, verbose: bool = False):
        """
        Тестирует все ключевые слова из базы
        """
        keywords = Keyword.objects.all().order_by('name')
        total = keywords.count()

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS(f"ТЕСТИРОВАНИЕ ВСЕХ КЛЮЧЕВЫХ СЛОВ ({total})"))
        self.stdout.write("=" * 70)

        stats = {
            'total': total,
            'normalized': 0,
            'unchanged': 0,
            'gaming_terms': 0,
            'short_words': 0,
            'in_wordnet': 0,
            'groups': defaultdict(list)
        }

        for i, kw in enumerate(keywords, 1):
            word = kw.name
            word_lower = word.lower()

            # Проверяем статус
            is_gaming = self.normalizer._is_gaming_term(word_lower)
            is_short = self.normalizer._is_short_word(word_lower)
            in_wordnet = bool(self.wn and self.wn.synsets(word_lower))
            base_form = self.normalizer._get_base_form(word_lower)

            if is_gaming:
                stats['gaming_terms'] += 1
            if is_short:
                stats['short_words'] += 1
            if in_wordnet:
                stats['in_wordnet'] += 1

            if base_form != word_lower:
                stats['normalized'] += 1
                stats['groups'][base_form].append(word)
            else:
                stats['unchanged'] += 1

            # Прогресс
            if i % 100 == 0:
                self.stdout.write(f"   Прогресс: {i}/{total}", ending='\r')

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("СТАТИСТИКА"))
        self.stdout.write("=" * 70)

        self.stdout.write(f"\n📊 Всего слов: {stats['total']}")
        self.stdout.write(f"   🔨 Нормализуются: {stats['normalized']} ({stats['normalized'] / total * 100:.1f}%)")
        self.stdout.write(f"   ⏺️ Не изменяются: {stats['unchanged']} ({stats['unchanged'] / total * 100:.1f}%)")
        self.stdout.write(f"   🎮 Игровые термины: {stats['gaming_terms']}")
        self.stdout.write(f"   📏 Короткие слова: {stats['short_words']}")
        self.stdout.write(f"   📖 В WordNet: {stats['in_wordnet']}")

        # Показываем топ групп
        if stats['groups']:
            self.stdout.write(f"\n📌 Топ-10 групп по размеру:")
            sorted_groups = sorted(stats['groups'].items(), key=lambda x: len(x[1]), reverse=True)[:10]

            for base, forms in sorted_groups:
                self.stdout.write(f"   • {base}: {len(forms)} форм")

                if verbose:
                    for form in forms[:5]:  # Показываем первые 5 форм
                        self.stdout.write(f"     - {form}")
                    if len(forms) > 5:
                        self.stdout.write(f"     ... и еще {len(forms) - 5}")

    def _compare_methods(self, words: list):
        """
        Сравнивает разные методы нормализации
        """
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("СРАВНЕНИЕ МЕТОДОВ НОРМАЛИЗАЦИИ"))
        self.stdout.write("=" * 70)

        # Таблица результатов
        self.stdout.write("\n{:<20} {:<20} {:<20} {:<20}".format(
            "Слово", "По правилам", "NLTK лемма", "Комбинированный"
        ))
        self.stdout.write("-" * 80)

        for word in words:
            rules = self.normalizer._normalize_by_rules(word)

            # NLTK лемматизация
            try:
                nltk_verb = self.normalizer.lemmatizer.lemmatize(word, 'v')
                nltk_noun = self.normalizer.lemmatizer.lemmatize(word, 'n')
                nltk_result = nltk_verb if nltk_verb != word else nltk_noun
            except:
                nltk_result = "ошибка"

            combined = self.normalizer._get_base_form(word)

            # Выделяем цветом если результаты отличаются
            if rules == combined == nltk_result:
                self.stdout.write("{:<20} {:<20} {:<20} {:<20}".format(
                    word, rules, nltk_result, combined
                ))
            else:
                self.stdout.write(self.style.WARNING("{:<20} {:<20} {:<20} {:<20}".format(
                    word, rules, nltk_result, combined
                )))

    def handle(self, *args, **options):
        words = options.get('words', [])
        test_all = options.get('test_all', False)
        check_group = options.get('check_group')
        verbose = options.get('verbose', False)
        compare = options.get('compare', False)

        start_time = time.time()

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("ТЕСТИРОВАНИЕ НОРМАЛИЗАЦИИ КЛЮЧЕВЫХ СЛОВ"))
        self.stdout.write("=" * 70)

        # Инициализируем нормализатор
        self._init_normalizer()

        # Тестовые слова по умолчанию
        test_words = ['stretching', 'drawing', 'running', 'swimming',
                      'played', 'playing', 'cities', 'boxes',
                      'safety', 'weightlessness', 'fps', 'rpg']

        if compare:
            # Режим сравнения методов
            self._compare_methods(words if words else test_words)

        elif check_group:
            # Режим проверки группы
            group = self._find_word_group(check_group)
            self._print_word_group(group)

        elif test_all:
            # Режим тестирования всех слов
            self._test_all_keywords(verbose)

        elif words:
            # Режим тестирования конкретных слов
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ТЕСТИРОВАНИЕ КОНКРЕТНЫХ СЛОВ"))
            self.stdout.write("=" * 70)

            for word in words:
                result = self._test_single_word(word, verbose)
                self._print_word_result(result, verbose)

        else:
            # Режим по умолчанию - тестируем стандартный набор
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ТЕСТИРОВАНИЕ СТАНДАРТНОГО НАБОРА"))
            self.stdout.write("=" * 70)

            for word in test_words:
                result = self._test_single_word(word, verbose)
                self._print_word_result(result, verbose)

        elapsed_time = time.time() - start_time
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(f"⏱️  Время выполнения: {elapsed_time:.2f} сек")
        self.stdout.write("=" * 70)