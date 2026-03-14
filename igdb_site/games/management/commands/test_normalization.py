# games/management/commands/test_normalization.py
"""
Тестовая команда для проверки нормализации ключевых слов
"""

from django.core.management.base import BaseCommand
from games.models import Keyword
from games.management.commands.normalize_keywords import Command as NormalizeCommand
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

        # Создаем экземпляр команды нормализации
        self.stdout.write("🔧 Создание экземпляра нормализатора...")
        normalize_cmd = NormalizeCommand()
        normalize_cmd.verbose = verbose

        self.stdout.write("✅ Нормализатор готов")
        self.stdout.write("")

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
            self.stdout.write("-" * 40)

            # Детальный вывод процесса нормализации
            self.stdout.write("\n   🔄 ПРОЦЕСС ПОИСКА ФОРМ:")

            # 1. Проверяем игровые термины
            is_gaming = normalize_cmd._is_gaming_term(word)
            self.stdout.write(f"     1. Проверка игрового термина: {'✅ ДА' if is_gaming else '❌ НЕТ'}")

            # 2. Проверяем короткие слова
            is_short = normalize_cmd._is_short_word(word)
            self.stdout.write(f"     2. Проверка короткого слова: {'✅ ДА' if is_short else '❌ НЕТ'}")

            if is_gaming or is_short:
                reason = "игровой термин" if is_gaming else "короткое слово"
                self.stdout.write(self.style.WARNING(f"\n   ⏺️ Досрочная остановка: {reason}"))
                self.stdout.write(f"\n   📊 ИТОГОВЫЕ ДАННЫЕ:")
                self.stdout.write(f"   🎯 Базовая форма: '{word}'")
                self.stdout.write(f"   🎮 Игровой термин: {'✅' if is_gaming else '❌'}")
                self.stdout.write(f"   📏 Короткое слово: {'✅' if is_short else '❌'}")
                self.stdout.write(self.style.WARNING(f"\n   ⏺️ НЕ БУДЕТ НОРМАЛИЗОВАНО ({reason})"))
                continue

            # 3. Получаем базовую форму
            self.stdout.write(f"     3. Вызов _get_base_form('{word}')...")
            base_form = normalize_cmd._get_base_form(word)
            self.stdout.write(f"        Результат: '{base_form}'")

            # 4. Проверяем семантическую связь
            self.stdout.write(f"\n     4. Вызов _are_semantically_related('{word}', '{base_form}')...")
            are_related = normalize_cmd._are_semantically_related(word, base_form)
            self.stdout.write(f"        Результат: {'✅ Связаны' if are_related else '❌ Не связаны'}")

            # 5. Проверяем, нужно ли нормализовать
            self.stdout.write(f"\n     5. Вызов _should_normalize('{word}', '{base_form}')...")
            should_normalize = normalize_cmd._should_normalize(word, base_form)
            self.stdout.write(f"        Результат: {'✅ Да' if should_normalize else '❌ Нет'}")

            # Итоговые данные
            self.stdout.write(f"\n   📊 ИТОГОВЫЕ ДАННЫЕ:")
            self.stdout.write(f"   🎯 Базовая форма: '{base_form}'")
            self.stdout.write(f"   🎮 Игровой термин: {'✅' if is_gaming else '❌'}")
            self.stdout.write(f"   📏 Короткое слово: {'✅' if is_short else '❌'}")
            self.stdout.write(f"   🔗 Семантическая связь: {'✅' if are_related else '❌'}")

            # Результат нормализации
            if should_normalize:
                self.stdout.write(self.style.SUCCESS(f"\n   ✅ БУДЕТ НОРМАЛИЗОВАНО: '{word}' → '{base_form}'"))
            else:
                reasons = []
                if is_gaming:
                    reasons.append("игровой термин")
                if is_short:
                    reasons.append("короткое слово")
                if word.lower() == base_form:
                    reasons.append("уже базовая форма")
                elif not are_related:
                    reasons.append("нет семантической связи")

                reason_str = ", ".join(reasons) if reasons else "неизвестная причина"
                self.stdout.write(self.style.WARNING(f"\n   ⏺️ НЕ БУДЕТ НОРМАЛИЗОВАНО ({reason_str})"))

        self.stdout.write("\n" + "=" * 70)