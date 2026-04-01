# games/management/commands/test_wordnet.py
"""
Тестовая команда для проверки слов в WordNet и семантической близости
"""

from django.core.management.base import BaseCommand
import nltk
from nltk.corpus import wordnet as wn
from itertools import combinations


class Command(BaseCommand):
    help = 'Проверяет наличие слов в WordNet и семантическую близость между словами'

    def add_arguments(self, parser):
        parser.add_argument(
            '--words',
            type=str,
            nargs='+',
            required=True,
            help='Список слов для проверки',
        )
        parser.add_argument(
            '--download',
            action='store_true',
            help='Скачать WordNet если отсутствует',
        )
        parser.add_argument(
            '--details',
            action='store_true',
            help='Показать подробную информацию о synsets',
        )
        parser.add_argument(
            '--check-relation',
            action='store_true',
            help='Проверить семантическую связь между всеми парами слов',
        )

    def _check_semantic_relation(self, word1: str, word2: str) -> dict:
        """
        Проверяет семантическую связь между двумя словами через WordNet
        Возвращает словарь с информацией о связи
        """
        result = {
            'related': False,
            'common_lemmas': [],
            'common_synsets': [],
            'path_similarity': 0.0,
            'wup_similarity': 0.0,
            'details': []
        }

        if word1 == word2:
            result['related'] = True
            result['details'].append("Одинаковые слова")
            return result

        try:
            word1_lower = word1.lower()
            word2_lower = word2.lower()

            # Получаем все synsets для обоих слов
            synsets1 = wn.synsets(word1_lower)
            synsets2 = wn.synsets(word2_lower)

            if not synsets1:
                result['details'].append(f"❌ '{word1}' не найдено в WordNet")
            else:
                result['details'].append(f"✅ '{word1}' найдено ({len(synsets1)} synsets)")

            if not synsets2:
                result['details'].append(f"❌ '{word2}' не найдено в WordNet")
            else:
                result['details'].append(f"✅ '{word2}' найдено ({len(synsets2)} synsets)")

            if not synsets1 or not synsets2:
                return result

            # Собираем все леммы для каждого слова
            lemmas1 = set()
            for syn in synsets1:
                for lemma in syn.lemmas():
                    lemma_name = lemma.name().lower().replace('_', ' ')
                    lemmas1.add(lemma_name)
                    # Также добавляем имя synset для проверки
                    result['common_synsets'].append(syn.name())

            lemmas2 = set()
            for syn in synsets2:
                for lemma in syn.lemmas():
                    lemma_name = lemma.name().lower().replace('_', ' ')
                    lemmas2.add(lemma_name)
                    result['common_synsets'].append(syn.name())

            # Находим общие леммы
            common_lemmas = lemmas1.intersection(lemmas2)

            if common_lemmas:
                result['related'] = True
                result['common_lemmas'] = list(common_lemmas)
                result['details'].append(f"✅ Общие леммы: {', '.join(common_lemmas)}")

            # Проверяем максимальную path similarity
            max_path_sim = 0.0
            max_wup_sim = 0.0

            for s1 in synsets1:
                for s2 in synsets2:
                    try:
                        path_sim = s1.path_similarity(s2)
                        if path_sim and path_sim > max_path_sim:
                            max_path_sim = path_sim

                        wup_sim = s1.wup_similarity(s2)
                        if wup_sim and wup_sim > max_wup_sim:
                            max_wup_sim = wup_sim
                    except:
                        continue

            result['path_similarity'] = round(max_path_sim, 3) if max_path_sim > 0 else 0.0
            result['wup_similarity'] = round(max_wup_sim, 3) if max_wup_sim > 0 else 0.0

            # Если высокая семантическая близость, считаем связанными
            PATH_THRESHOLD = 0.3
            WUP_THRESHOLD = 0.7

            if max_path_sim > PATH_THRESHOLD:
                result['related'] = True
                result['details'].append(
                    f"✅ Path similarity выше порога: {result['path_similarity']} > {PATH_THRESHOLD}")

            if max_wup_sim > WUP_THRESHOLD:
                result['related'] = True
                result['details'].append(f"✅ WUP similarity выше порога: {result['wup_similarity']} > {WUP_THRESHOLD}")

        except Exception as e:
            result['details'].append(f"❌ Ошибка: {e}")

        return result

    def handle(self, *args, **options):
        words = options['words']
        download = options['download']
        details = options['details']
        check_relation = options['check_relation']

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("ТЕСТИРОВАНИЕ WORDNET"))
        self.stdout.write("=" * 70)

        # Проверяем наличие WordNet
        try:
            wn.synsets('test')
        except LookupError:
            if download:
                self.stdout.write("📥 Скачиваем WordNet...")
                nltk.download('wordnet', quiet=False)
                nltk.download('omw-1.4', quiet=False)
                self.stdout.write(self.style.SUCCESS("✅ WordNet загружен"))
            else:
                self.stdout.write(self.style.ERROR("❌ WordNet не найден. Используйте --download для загрузки"))
                return

        # Проверяем каждое слово
        for word in words:
            word_lower = word.lower()

            self.stdout.write(f"\n📌 Слово: '{word}'")
            self.stdout.write("-" * 50)

            # Получаем все synsets для слова
            synsets = wn.synsets(word_lower)

            if not synsets:
                self.stdout.write(self.style.WARNING(f"   ❌ НЕТ в WordNet"))

                # Проверяем разные части речи отдельно
                for pos in ['n', 'v', 'a', 'r']:  # noun, verb, adjective, adverb
                    pos_synsets = wn.synsets(word_lower, pos=pos)
                    if pos_synsets:
                        pos_names = {'n': 'существительное', 'v': 'глагол', 'a': 'прилагательное', 'r': 'наречие'}
                        self.stdout.write(f"      • Как {pos_names[pos]}: {len(pos_synsets)} synsets")
            else:
                self.stdout.write(self.style.SUCCESS(f"   ✅ ЕСТЬ в WordNet ({len(synsets)} synsets)"))

                # Группируем по частям речи
                by_pos = {'n': [], 'v': [], 'a': [], 'r': [], 's': []}
                for syn in synsets:
                    by_pos[syn.pos()].append(syn)

                for pos, syns in by_pos.items():
                    if syns:
                        pos_names = {'n': 'существительное', 'v': 'глагол',
                                     'a': 'прилагательное', 'r': 'наречие', 's': 'прилагательное (satellite)'}
                        self.stdout.write(f"      • {pos_names.get(pos, pos)}: {len(syns)} synsets")

            if details and synsets:
                self.stdout.write(f"\n   📖 Подробная информация:")
                for i, syn in enumerate(synsets[:3], 1):  # Показываем первые 3
                    self.stdout.write(f"      {i}. {syn.name()} - {syn.definition()}")
                    if syn.examples():
                        examples = '; '.join(syn.examples()[:2])
                        self.stdout.write(f"         Примеры: {examples}")

        # Проверяем семантическую связь между всеми парами слов
        if check_relation and len(words) >= 2:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ПРОВЕРКА СЕМАНТИЧЕСКОЙ СВЯЗИ МЕЖДУ ВСЕМИ ПАРАМИ"))
            self.stdout.write("=" * 70)

            for word1, word2 in combinations(words, 2):
                self.stdout.write(f"\n🔍 {word1} ↔ {word2}")

                result = self._check_semantic_relation(word1, word2)

                if result['related']:
                    self.stdout.write(self.style.SUCCESS(f"   ✅ СВЯЗАНЫ"))
                    if result['common_lemmas']:
                        self.stdout.write(f"      Общие леммы: {', '.join(result['common_lemmas'])}")
                    self.stdout.write(f"      Path similarity: {result['path_similarity']}")
                    if details:
                        self.stdout.write(f"      WUP similarity: {result['wup_similarity']}")
                else:
                    self.stdout.write(self.style.WARNING(f"   ❌ НЕ СВЯЗАНЫ"))
                    self.stdout.write(f"      Path similarity: {result['path_similarity']}")