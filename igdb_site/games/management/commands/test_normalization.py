# games/management/commands/test_normalization.py
"""
Тестовая команда для проверки нормализации ключевых слов
"""

from django.core.management.base import BaseCommand
from games.models import Keyword
from .normalize_keywords import Command as NormalizeCommand
import time


class Command(BaseCommand):
    help = 'Тестирует нормализацию конкретных слов'

    def add_arguments(self, parser):
        parser.add_argument(
            '--words',
            type=str,
            nargs='+',
            required=True,
            help='Список слов для тестирования',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод',
        )
        parser.add_argument(
            '--check-db',
            action='store_true',
            help='Проверить наличие слов в базе данных',
        )

    def handle(self, *args, **options):
        words = options['words']
        verbose = options['verbose']
        check_db = options['check_db']

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("ТЕСТИРОВАНИЕ НОРМАЛИЗАЦИИ СЛОВ"))
        self.stdout.write("=" * 70)

        # Создаем экземпляр команды нормализации для доступа к методам
        normalize_cmd = NormalizeCommand()
        normalize_cmd._init_nltk()

        # Проверяем наличие в базе если нужно
        if check_db:
            self.stdout.write("\n🔍 ПРОВЕРКА НАЛИЧИЯ В БАЗЕ:")
            self.stdout.write("-" * 50)

            for word in words:
                exists = Keyword.objects.filter(name__iexact=word).exists()
                if exists:
                    kw = Keyword.objects.get(name__iexact=word)
                    games_count = kw.game_set.count()
                    self.stdout.write(f"📌 '{word}': ✅ ЕСТЬ в базе (ID: {kw.id}, игр: {games_count})")
                else:
                    self.stdout.write(f"📌 '{word}': ❌ НЕТ в базе")

        self.stdout.write("\n🔍 ТЕСТИРОВАНИЕ НОРМАЛИЗАЦИИ:")
        self.stdout.write("-" * 50)

        for word in words:
            self.stdout.write(f"\n📌 Слово: '{word}'")

            # Проверяем, является ли игровым термином
            is_gaming = normalize_cmd._is_gaming_term(word)

            # Проверяем, короткое ли слово
            is_short = normalize_cmd._is_short_word(word)

            if is_gaming or is_short:
                self.stdout.write(self.style.WARNING(f"   ⏺️ НЕ ИЗМЕНЯЕТСЯ {word} = {word}"))
                continue

            # Получаем базовую форму
            base_form = normalize_cmd._get_base_form(word)

            # Проверяем семантическую связь
            try:
                from nltk.corpus import wordnet as wn

                synsets1 = wn.synsets(word.lower())
                synsets2 = wn.synsets(base_form.lower())

                if synsets1 and synsets2:
                    # Проверяем path similarity
                    max_path_sim = 0.0

                    for s1 in synsets1:
                        for s2 in synsets2:
                            try:
                                path_sim = s1.path_similarity(s2)
                                if path_sim and path_sim > max_path_sim:
                                    max_path_sim = path_sim
                            except:
                                continue

                    PATH_THRESHOLD = normalize_cmd.PATH_SIMILARITY_THRESHOLD

                    # Всегда показываем path similarity и порог
                    self.stdout.write(f"   Path similarity: {max_path_sim:.3f} (порог: {PATH_THRESHOLD})")

                    if verbose:
                        # Показываем WUP similarity только в подробном режиме
                        max_wup_sim = 0.0
                        for s1 in synsets1:
                            for s2 in synsets2:
                                try:
                                    wup_sim = s1.wup_similarity(s2)
                                    if wup_sim and wup_sim > max_wup_sim:
                                        max_wup_sim = wup_sim
                                except:
                                    continue
                        self.stdout.write(
                            f"   WUP similarity: {max_wup_sim:.3f} (порог: {normalize_cmd.WUP_SIMILARITY_THRESHOLD})")

            except Exception as e:
                if verbose:
                    self.stdout.write(f"   ❌ Ошибка: {e}")

            # Результат нормализации
            if base_form == word.lower():
                self.stdout.write(self.style.WARNING(f"   ⏺️ НЕ ИЗМЕНЯЕТСЯ {word} = {word}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"   ✅ ИЗМЕНЯЕТСЯ {word} → {base_form}"))

        self.stdout.write("\n" + "=" * 70)