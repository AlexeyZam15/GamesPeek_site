# games/management/commands/test_words_in_wordnet.py
"""
Простая команда для проверки наличия слов в WordNet
"""

from django.core.management.base import BaseCommand
import nltk
from nltk.corpus import wordnet


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

    def _ensure_wordnet(self):
        """Гарантирует, что WordNet загружен"""
        try:
            wordnet.synsets('test')
        except LookupError:
            self.stdout.write("📥 Загружаем WordNet...")
            nltk.download('wordnet', quiet=True)
            nltk.download('omw-1.4', quiet=True)

    def handle(self, *args, **options):
        words = options['words']

        self._ensure_wordnet()

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("ПРОВЕРКА СЛОВ В WORDNET")
        self.stdout.write("=" * 60)

        for word in words:
            # Проверяем наличие в WordNet
            synsets = wordnet.synsets(word.lower())
            exists = len(synsets) > 0

            if exists:
                self.stdout.write(f"✅ {word}: ЕСТЬ в WordNet ({len(synsets)} synsets)")
            else:
                self.stdout.write(f"❌ {word}: НЕТ в WordNet")

        self.stdout.write("=" * 60)