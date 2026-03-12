# games/management/commands/test_wordnet.py
"""
Тестовая команда для проверки слов в WordNet
"""

from django.core.management.base import BaseCommand
import nltk
from nltk.corpus import wordnet as wn


class Command(BaseCommand):
    help = 'Проверяет наличие слов в WordNet'

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

    def handle(self, *args, **options):
        words = options['words']
        download = options['download']
        details = options['details']

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

        # Проверяем hitpoint и связанные слова
        if 'hitpoint' in [w.lower() for w in words] or 'hitpoints' in [w.lower() for w in words]:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ДЛЯ hitpoint/hitpoints"))
            self.stdout.write("=" * 70)

            # Проверяем составные части
            for part in ['hit', 'point', 'points']:
                part_synsets = wn.synsets(part)
                self.stdout.write(
                    f"\n📌 Часть: '{part}' - {'✅ ЕСТЬ' if part_synsets else '❌ НЕТ'} ({len(part_synsets)} synsets)")

                if part_synsets and details:
                    for i, syn in enumerate(part_synsets[:2], 1):
                        self.stdout.write(f"      {i}. {syn.name()} - {syn.definition()}")

            # Проверяем возможные варианты написания
            variants = ['hit-point', 'hit point', 'hit_point', 'hitpoints']
            self.stdout.write(f"\n📌 Варианты написания:")
            for var in variants:
                var_synsets = wn.synsets(var)
                status = '✅ ЕСТЬ' if var_synsets else '❌ НЕТ'
                self.stdout.write(f"      • '{var}': {status}")