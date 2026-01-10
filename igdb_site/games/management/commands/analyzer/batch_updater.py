# games/management/commands/analyzer/batch_updater.py
"""
Батч-апдейтер для обновления игр (как в старой версии)
"""

from typing import Dict, Any, List
from django.db import transaction


class BatchUpdater:
    """Батч-апдейтер для обновления игр в базе"""

    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.games_to_update: List[Dict] = []

    def add_game_for_update(self, game_id: int, results: Dict[str, Any], is_keywords: bool):
        """Добавляет игру для обновления"""
        try:
            # Проверяем, есть ли результаты для обновления
            has_actual_results = False
            total_elements = 0

            if is_keywords:
                keywords = results.get('keywords', {}).get('items', [])
                if keywords:
                    has_actual_results = True
                    total_elements = len(keywords)
            else:
                for key in ['genres', 'themes', 'perspectives', 'game_modes']:
                    items = results.get(key, {}).get('items', [])
                    if items:
                        has_actual_results = True
                        total_elements += len(items)

            if not has_actual_results or total_elements == 0:
                return 0  # Возвращаем 0

            # Добавляем игру в батч
            self.games_to_update.append({
                'game_id': game_id,
                'results': results,
                'is_keywords': is_keywords,
                'element_count': total_elements
            })

            # Обновляем только когда батч заполнен
            if len(self.games_to_update) >= self.batch_size:
                updated_games = self._execute_batch_update()
                return updated_games  # Возвращаем количество ОБНОВЛЕННЫХ ИГР

            return 0  # Игра добавлена в батч, но еще не обновлена

        except Exception as e:
            import sys
            return 0

    def flush(self) -> int:
        """Обновляет оставшиеся игры - возвращает количество ОБНОВЛЕННЫХ ИГР"""
        if not self.games_to_update:
            return 0

        updated_games = self._execute_batch_update()  # Теперь возвращает количество ОБНОВЛЕННЫХ ИГР

        return updated_games

    @transaction.atomic
    def _execute_batch_update(self) -> int:
        """Выполняет обновление накопленного батча - возвращает количество ОБНОВЛЕННЫХ ИГР"""
        if not self.games_to_update:
            return 0

        from games.models import Game, Keyword, Genre, Theme, PlayerPerspective, GameMode

        updated_games_count = 0  # Счетчик ОБНОВЛЕННЫХ ИГР (не элементов)
        updated_elements_count = 0  # Счетчик обновленных элементов (для информации)
        errors = 0

        for game_data in self.games_to_update:
            try:
                game = Game.objects.get(id=game_data['game_id'])
                was_game_updated = False  # Флаг: была ли обновлена эта игра

                if game_data['is_keywords']:
                    # Обновляем ключевые слова
                    keywords = game_data['results'].get('keywords', {}).get('items', [])
                    if keywords:
                        keyword_ids = [k['id'] for k in keywords]
                        keyword_objects = Keyword.objects.filter(id__in=keyword_ids)
                        if keyword_objects.exists():
                            count_before = game.keywords.count()
                            game.keywords.add(*keyword_objects)
                            count_after = game.keywords.count()
                            added_count = count_after - count_before

                            if added_count > 0:
                                updated_elements_count += added_count
                                was_game_updated = True
                else:
                    # Обновляем критерии
                    # Жанры
                    genres = game_data['results'].get('genres', {}).get('items', [])
                    if genres:
                        genre_ids = [g['id'] for g in genres]
                        genre_objects = Genre.objects.filter(id__in=genre_ids)
                        if genre_objects.exists():
                            count_before = game.genres.count()
                            game.genres.add(*genre_objects)
                            count_after = game.genres.count()
                            added_count = count_after - count_before

                            if added_count > 0:
                                updated_elements_count += added_count
                                was_game_updated = True

                    # Темы
                    themes = game_data['results'].get('themes', {}).get('items', [])
                    if themes:
                        theme_ids = [t['id'] for t in themes]
                        theme_objects = Theme.objects.filter(id__in=theme_ids)
                        if theme_objects.exists():
                            count_before = game.themes.count()
                            game.themes.add(*theme_objects)
                            count_after = game.themes.count()
                            added_count = count_after - count_before

                            if added_count > 0:
                                updated_elements_count += added_count
                                was_game_updated = True

                    # Перспективы
                    perspectives = game_data['results'].get('perspectives', {}).get('items', [])
                    if perspectives:
                        perspective_ids = [p['id'] for p in perspectives]
                        perspective_objects = PlayerPerspective.objects.filter(id__in=perspective_ids)
                        if perspective_objects.exists():
                            count_before = game.player_perspectives.count()
                            game.player_perspectives.add(*perspective_objects)
                            count_after = game.player_perspectives.count()
                            added_count = count_after - count_before

                            if added_count > 0:
                                updated_elements_count += added_count
                                was_game_updated = True

                    # Режимы игры
                    game_modes = game_data['results'].get('game_modes', {}).get('items', [])
                    if game_modes:
                        mode_ids = [m['id'] for m in game_modes]
                        mode_objects = GameMode.objects.filter(id__in=mode_ids)
                        if mode_objects.exists():
                            count_before = game.game_modes.count()
                            game.game_modes.add(*mode_objects)
                            count_after = game.game_modes.count()
                            added_count = count_after - count_before

                            if added_count > 0:
                                updated_elements_count += added_count
                                was_game_updated = True

                if was_game_updated:
                    updated_games_count += 1  # Увеличиваем счетчик ОБНОВЛЕННЫХ ИГР
                    # Обновляем кэшированные счетчики
                    try:
                        game.update_cached_counts(force=True)
                        game.save()
                    except:
                        pass

            except Game.DoesNotExist:
                errors += 1
            except Exception:
                errors += 1

        # Очищаем батч
        self.games_to_update.clear()

        # Возвращаем количество ОБНОВЛЕННЫХ ИГР
        return updated_games_count
