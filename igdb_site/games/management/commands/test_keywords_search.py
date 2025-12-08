# games/management/commands/test_keywords_search.py
from django.core.management.base import BaseCommand
from games.models import Game, Keyword
import re
import random


class Command(BaseCommand):
    help = 'Тестирование поиска ключевых слов в нескольких играх'

    def handle(self, *args, **options):
        # Получаем все ключевые слова
        all_keywords = list(Keyword.objects.all())
        keyword_lower_map = {kw.name.lower(): kw for kw in all_keywords}

        # Берем 5 случайных игр с текстом
        games_with_text = Game.objects.exclude(summary='').exclude(storyline='')
        if games_with_text.count() == 0:
            self.stdout.write("❌ Нет игр с текстом для анализа")
            return

        test_games = list(games_with_text.order_by('?')[:5])  # 5 случайных игр

        self.stdout.write(f"🔍 Тестируем поиск ключевых слов в {len(test_games)} играх")
        self.stdout.write("=" * 60)

        total_found = 0
        games_with_keywords = 0

        for i, game in enumerate(test_games, 1):
            self.stdout.write(f"\n{i}. 🎮 Игра: {game.name} (ID: {game.id})")

            # Получаем текст
            text = game.summary or game.storyline or ""
            text_lower = text.lower()

            self.stdout.write(f"   📝 Длина текста: {len(text)} символов")

            # Ищем ключевые слова
            found_keywords = []

            for keyword_lower, keyword_obj in keyword_lower_map.items():
                if ' ' in keyword_lower:
                    # Многословное ключевое слово
                    if keyword_lower in text_lower:
                        found_keywords.append(keyword_obj)
                else:
                    # Однословное ключевое слово
                    if re.search(rf'\b{re.escape(keyword_lower)}\b', text_lower):
                        found_keywords.append(keyword_obj)

            if found_keywords:
                games_with_keywords += 1
                total_found += len(found_keywords)
                self.stdout.write(f"   ✅ Найдено ключевых слов: {len(found_keywords)}")

                # Показываем первые 5 найденных
                for kw in found_keywords[:5]:
                    self.stdout.write(f"     • {kw.name}")

                if len(found_keywords) > 5:
                    self.stdout.write(f"     ... и еще {len(found_keywords) - 5}")
            else:
                self.stdout.write(f"   ❌ Ключевые слова не найдены")

                # Покажем превью текста
                preview = text[:150] + "..." if len(text) > 150 else text
                self.stdout.write(f"   📄 Текст: {preview}")

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("📊 ИТОГИ ТЕСТА:")
        self.stdout.write(f"   Всего протестировано игр: {len(test_games)}")
        self.stdout.write(f"   Игр с найденными ключевыми словами: {games_with_keywords}")
        self.stdout.write(f"   Всего найдено ключевых слов: {total_found}")

        if games_with_keywords == 0:
            self.stdout.write("\n⚠️ ПРОБЛЕМА: Ни в одной игре не найдены ключевые слова")
            self.stdout.write("\nВозможные причины:")
            self.stdout.write("1. Ключевые слова слишком специфичны")
            self.stdout.write("2. Тексты игр не содержат этих ключевых слов")
            self.stdout.write("3. Проблема с алгоритмом поиска")

            # Тестируем конкретные ключевые слова
            self.stdout.write("\n🔍 Тестируем конкретные ключевые слова:")
            test_keywords = ['rpg', 'action', 'adventure', 'multiplayer', '3d', 'fantasy']

            for test_game in test_games[:2]:  # Первые 2 игры
                text = (test_game.summary or test_game.storyline or "").lower()
                self.stdout.write(f"\n   Игра: {test_game.name}")
                for kw in test_keywords:
                    if kw in text:
                        self.stdout.write(f"     ✅ '{kw}' найдено")
                    else:
                        self.stdout.write(f"     ❌ '{kw}' не найдено")