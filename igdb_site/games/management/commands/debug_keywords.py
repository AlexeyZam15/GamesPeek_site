# games/management/commands/debug_keywords.py
from django.core.management.base import BaseCommand
from games.models import Game, Keyword
import re


class Command(BaseCommand):
    help = 'Отладка поиска ключевых слов'

    def add_arguments(self, parser):
        parser.add_argument('--game-id', type=int, help='ID игры для теста')
        parser.add_argument('--keyword', type=str, help='Ключевое слово для поиска')
        parser.add_argument('--test-all', action='store_true', help='Тестировать все ключевые слова')

    def handle(self, *args, **options):
        # Показываем статистику ключевых слов
        total_keywords = Keyword.objects.count()
        self.stdout.write(f"📊 Всего ключевых слов в базе: {total_keywords}")

        if total_keywords == 0:
            self.stdout.write("❌ В базе нет ключевых слов!")
            self.stdout.write("ℹ️ Добавьте ключевые слова через админку Django")
            return

        # Показываем первые 20 ключевых слов
        self.stdout.write("\n🔍 Первые 20 ключевых слов:")
        keywords_list = list(Keyword.objects.all()[:20])
        for i, kw in enumerate(keywords_list, 1):
            self.stdout.write(f"  {i:2d}. {kw.name}")

        # Если указана игра, тестируем на ней
        if options['game_id']:
            try:
                game = Game.objects.get(id=options['game_id'])
                self.stdout.write(f"\n🎮 Тестируем игру: {game.name} (ID: {game.id})")

                # Получаем текст для анализа
                text = game.summary or game.storyline or ""
                if not text.strip():
                    self.stdout.write("❌ У игры нет текста для анализа")
                    return

                self.stdout.write(f"📝 Текст для анализа ({len(text)} символов):")
                preview = text[:200] + "..." if len(text) > 200 else text
                self.stdout.write(f"   {preview}")

                # Преобразуем текст в нижний регистр для поиска
                text_lower = text.lower()

                # Ищем ключевые слова простым способом
                found_keywords = []

                for keyword in Keyword.objects.all():
                    keyword_lower = keyword.name.lower()

                    # Проверяем, есть ли ключевое слово в тексте
                    if ' ' in keyword_lower:
                        # Многословное ключевое слово
                        if keyword_lower in text_lower:
                            found_keywords.append(keyword)
                    else:
                        # Однословное ключевое слово - ищем как отдельное слово
                        pattern = rf'\b{re.escape(keyword_lower)}\b'
                        if re.search(pattern, text_lower):
                            found_keywords.append(keyword)

                self.stdout.write(f"\n🔑 Найдено ключевых слов: {len(found_keywords)}")

                if found_keywords:
                    self.stdout.write("📌 Найденные ключевые слова:")
                    for keyword in found_keywords[:20]:  # Показываем первые 20
                        self.stdout.write(f"  • {keyword.name}")

                    if len(found_keywords) > 20:
                        self.stdout.write(f"  ... и еще {len(found_keywords) - 20}")
                else:
                    self.stdout.write("❌ Ключевые слова не найдены")

                    # Показываем примеры ключевых слов для проверки
                    self.stdout.write("\n🔍 Примеры ключевых слов для проверки:")
                    sample_keywords = Keyword.objects.all()[:10]
                    for kw in sample_keywords:
                        kw_lower = kw.name.lower()
                        if ' ' in kw_lower:
                            found = kw_lower in text_lower
                        else:
                            found = re.search(rf'\b{re.escape(kw_lower)}\b', text_lower) is not None

                        status = "✅" if found else "❌"
                        self.stdout.write(f"  {status} '{kw.name}'")

            except Game.DoesNotExist:
                self.stderr.write(f"❌ Игра с ID {options['game_id']} не найдена")

        # Если указано ключевое слово, проверяем его
        elif options['keyword']:
            keyword = options['keyword'].strip()
            exists = Keyword.objects.filter(name__iexact=keyword).exists()
            self.stdout.write(f"\n🔍 Проверка ключевого слова '{keyword}':")
            self.stdout.write(f"  {'✅ Найдено в базе' if exists else '❌ Не найдено в базе'}")

            if not exists:
                # Проверяем похожие ключевые слова
                similar = Keyword.objects.filter(name__icontains=keyword)[:5]
                if similar:
                    self.stdout.write("  Похожие ключевые слова:")
                    for kw in similar:
                        self.stdout.write(f"    • {kw.name}")

        # Если тестировать все ключевые слова
        elif options['test_all']:
            self.stdout.write("\n🧪 Тестируем все ключевые слова на примере текста...")

            # Пример текста для теста
            test_text = "This is an RPG game with sci-fi elements, action combat and multiplayer mode. It features 3D graphics and open world exploration."
            test_text_lower = test_text.lower()

            self.stdout.write(f"📝 Тестовый текст: {test_text}")

            found_count = 0
            for keyword in Keyword.objects.all():
                keyword_lower = keyword.name.lower()

                if ' ' in keyword_lower:
                    found = keyword_lower in test_text_lower
                else:
                    found = re.search(rf'\b{re.escape(keyword_lower)}\b', test_text_lower) is not None

                if found:
                    found_count += 1
                    if found_count <= 10:  # Показываем первые 10 найденных
                        self.stdout.write(f"  ✅ '{keyword.name}'")

            self.stdout.write(f"\n📊 Найдено ключевых слов в тестовом тексте: {found_count}")

            if found_count == 0:
                self.stdout.write("\n⚠️ Проблемы с поиском:")
                self.stdout.write("   1. Проверьте регистр (поиск должен быть нечувствителен к регистру)")
                self.stdout.write("   2. Проверьте разделители слов")
                self.stdout.write("   3. Убедитесь, что ключевые слова в базе корректны")

        else:
            # Показываем справку, если нет аргументов
            self.stdout.write("\n📖 Примеры использования:")
            self.stdout.write("  python manage.py debug_keywords --game-id 1")
            self.stdout.write("  python manage.py debug_keywords --keyword 'rpg'")
            self.stdout.write("  python manage.py debug_keywords --test-all")