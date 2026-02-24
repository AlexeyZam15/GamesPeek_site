# games/management/commands/debug_similarity.py
from django.core.management.base import BaseCommand
from games.models import Game
from games.similarity import GameSimilarity
from django.db import connection
import time


class Command(BaseCommand):
    help = 'Дебаг поиска похожих игр'

    def add_arguments(self, parser):
        parser.add_argument('game_id', type=int, help='ID игры для дебага')
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Количество кандидатов для показа (по умолчанию: 50)'
        )

    def handle(self, *args, **options):
        game_id = options['game_id']
        limit = options['limit']

        try:
            game = Game.objects.get(id=game_id)
        except Game.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Игра с ID {game_id} не найдена"))
            return

        sim = GameSimilarity()

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 80}'))
        self.stdout.write(self.style.SUCCESS(f'ДЕБАГ ПОИСКА ПОХОЖИХ ИГР ДЛЯ: {game.name} (ID: {game.id})'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 80}\n'))

        # 1. Подготовка данных
        self.stdout.write("1. ПОДГОТОВКА ДАННЫХ ИСХОДНОЙ ИГРЫ")
        self.stdout.write("-" * 50)

        source_data, single_player_info = sim._prepare_source_data(game)

        self.stdout.write(f"Жанры: {source_data['genre_ids']}")
        self.stdout.write(f"Ключевые слова: {len(source_data['keyword_ids'])} шт.")
        self.stdout.write(f"Движки: {source_data['engine_ids']}")
        self.stdout.write(f"Single player: {single_player_info['has_single_player']}")
        self.stdout.write(f"Мин. общих жанров: {single_player_info['dynamic_min_common_genres']}")

        # 2. Поиск кандидатов
        self.stdout.write("\n2. ПОИСК КАНДИДАТОВ")
        self.stdout.write("-" * 50)

        candidate_ids = sim._get_candidate_ids_new(source_data, single_player_info, sim.DEFAULT_MIN_SIMILARITY)
        self.stdout.write(f"Найдено кандидатов: {len(candidate_ids)}")

        # 3. Подготовка данных кандидатов
        self.stdout.write("\n3. ПОДГОТОВКА ДАННЫХ КАНДИДАТОВ")
        self.stdout.write("-" * 50)

        games_data = sim._prepare_candidate_data(candidate_ids)
        self.stdout.write(f"Подготовлено данных: {len(games_data)} игр")

        # 4. Подсчет общих элементов
        self.stdout.write("\n4. ПОДСЧЕТ ОБЩИХ ЭЛЕМЕНТОВ")
        self.stdout.write("-" * 50)

        games_data = sim._calculate_common_elements_new(games_data, source_data, candidate_ids)

        # 5. Расчет схожести
        self.stdout.write("\n5. РАСЧЕТ СХОЖЕСТИ")
        self.stdout.write("-" * 50)

        similar_games = []
        source_genre_count = source_data['genre_count']
        source_keyword_count = source_data['keyword_count']
        source_theme_count = source_data['theme_count']
        source_developer_count = source_data['developer_count']
        source_perspective_count = source_data['perspective_count']
        source_game_mode_count = source_data['game_mode_count']
        source_engine_count = source_data['engine_count']

        has_genres = source_genre_count > 0
        dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
        has_single_player = single_player_info['has_single_player']
        min_similarity = sim.DEFAULT_MIN_SIMILARITY

        self.stdout.write(f"Порог схожести: {min_similarity}%")
        self.stdout.write(f"Требование по жанрам: {dynamic_min_common_genres if has_genres else 'нет'}")
        self.stdout.write(f"Требование Single player: {has_single_player}")

        passed = 0
        failed_genre = 0
        failed_sp = 0
        failed_similarity = 0

        # Получаем названия для всех кандидатов одним запросом
        all_candidate_ids = list(games_data.keys())
        candidates_names = {}
        if all_candidate_ids:
            candidates = Game.objects.filter(id__in=all_candidate_ids).values('id', 'name')
            candidates_names = {c['id']: c['name'] for c in candidates}

        for game_id, data in games_data.items():
            # Если это исходная игра
            if game_id == game.id:
                similar_games.append({
                    'game_id': game_id,
                    'similarity': 100.0,
                    'passed': True,
                    'reason': 'source_game'
                })
                continue

            # Проверка по жанрам
            if has_genres and data['common_genres'] < dynamic_min_common_genres:
                failed_genre += 1
                continue

            # Проверка по Single player
            if has_single_player and not data['has_single_player']:
                failed_sp += 1
                continue

            # Расчет схожести
            similarity = sim._calculate_game_similarity_new(
                source_genre_count, source_keyword_count, source_theme_count,
                source_developer_count, source_perspective_count, source_game_mode_count,
                source_engine_count,
                data, source_data
            )

            if similarity >= min_similarity:
                similar_games.append({
                    'game_id': game_id,
                    'similarity': similarity,
                    'passed': True,
                    'reason': 'similarity'
                })
                passed += 1
            else:
                failed_similarity += 1

        self.stdout.write(f"\nРезультаты фильтрации:")
        self.stdout.write(f"  Прошло: {passed}")
        self.stdout.write(f"  Отсеяно по жанрам: {failed_genre}")
        self.stdout.write(f"  Отсеяно по Single player: {failed_sp}")
        self.stdout.write(f"  Отсеяно по схожести: {failed_similarity}")

        # Покажем игры, которые прошли
        if similar_games:
            self.stdout.write(f"\nИГРЫ, ПРОШЕДШИЕ ФИЛЬТРАЦИЮ ({len(similar_games)}):")
            self.stdout.write("-" * 50)

            # Сортируем по схожести
            similar_games.sort(key=lambda x: x['similarity'], reverse=True)

            for item in similar_games[:10]:  # первые 10
                gid = item['game_id']
                name = candidates_names.get(gid, "Unknown")
                sim_val = item['similarity']
                reason = item['reason']

                if gid == game.id:
                    self.stdout.write(self.style.SUCCESS(f"  🎯 {name} (ID: {gid}) - {sim_val:.1f}% (ИСХОДНАЯ)"))
                else:
                    color = self.style.SUCCESS if sim_val >= 70 else self.style.WARNING
                    self.stdout.write(color(f"  ✓ {name} (ID: {gid}) - {sim_val:.1f}%"))

        # Проверим конкретно San Andreas если это GTA V
        if game.name == "Grand Theft Auto V":
            try:
                sa = Game.objects.get(name__icontains="San Andreas")
                if sa.id in games_data:
                    data = games_data[sa.id]
                    self.stdout.write(f"\n{'=' * 50}")
                    self.stdout.write(self.style.WARNING("ПРОВЕРКА GRAND THEFT AUTO: SAN ANDREAS"))
                    self.stdout.write(f"{'=' * 50}")

                    self.stdout.write(
                        f"Общих жанров: {data['common_genres']} (требуется: {dynamic_min_common_genres if has_genres else 'нет'})")
                    self.stdout.write(f"Общих ключевых слов: {data['common_keywords']}")
                    self.stdout.write(f"Single player: {data['has_single_player']} (требуется: {has_single_player})")

                    if has_genres and data['common_genres'] < dynamic_min_common_genres:
                        self.stdout.write(self.style.ERROR(f"❌ Отсеяно по жанрам"))
                    elif has_single_player and not data['has_single_player']:
                        self.stdout.write(self.style.ERROR(f"❌ Отсеяно по Single player"))
                    else:
                        similarity = sim._calculate_game_similarity_new(
                            source_genre_count, source_keyword_count, source_theme_count,
                            source_developer_count, source_perspective_count, source_game_mode_count,
                            source_engine_count,
                            data, source_data
                        )
                        self.stdout.write(f"Рассчитанная схожесть: {similarity:.1f}%")

                        if similarity >= min_similarity:
                            self.stdout.write(
                                self.style.SUCCESS(f"✅ Прошло! Схожесть {similarity:.1f}% >= {min_similarity}%"))
                        else:
                            self.stdout.write(
                                self.style.ERROR(f"❌ Отсеяно по схожести: {similarity:.1f}% < {min_similarity}%"))
                else:
                    self.stdout.write(self.style.ERROR("San Andreas не найден в кандидатах!"))

                    # Проверим, есть ли он вообще в базе
                    if Game.objects.filter(id=sa.id).exists():
                        self.stdout.write(f"San Andreas (ID: {sa.id}) есть в базе, но не попал в кандидаты")

                        # Проверим жанры San Andreas
                        sa_genres = list(sa.genres.values_list('id', flat=True))
                        common = set(source_data['genre_ids']) & set(sa_genres)
                        self.stdout.write(f"Общих жанров с GTA V: {len(common)}")
                        self.stdout.write(f"Общие жанры ID: {common}")

            except Game.DoesNotExist:
                pass

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 80}'))