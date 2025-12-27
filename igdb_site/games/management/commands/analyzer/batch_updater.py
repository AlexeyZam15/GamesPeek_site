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
        self.games_to_update.append({
            'game_id': game_id,
            'results': results,
            'is_keywords': is_keywords
        })

        # Если накопился батч, выполняем обновление
        if len(self.games_to_update) >= self.batch_size:
            return self._execute_batch_update()

        return 0

    def flush(self) -> int:
        """Обновляет оставшиеся игры"""
        return self._execute_batch_update()

    @transaction.atomic
    def _execute_batch_update(self) -> int:
        """Выполняет обновление накопленного батча"""
        if not self.games_to_update:
            return 0

        from games.models import Game, Keyword, Genre, Theme, PlayerPerspective, GameMode

        updated_count = 0

        for game_data in self.games_to_update:
            try:
                game = Game.objects.get(id=game_data['game_id'])

                if game_data['is_keywords']:
                    # Обновляем ключевые слова
                    keywords = game_data['results'].get('keywords', {}).get('items', [])
                    if keywords:
                        keyword_ids = [k['id'] for k in keywords]
                        keyword_objects = Keyword.objects.filter(id__in=keyword_ids)
                        game.keywords.add(*keyword_objects)
                        updated_count += 1
                else:
                    # Обновляем критерии
                    updated = False

                    # Жанры
                    genres = game_data['results'].get('genres', {}).get('items', [])
                    if genres:
                        genre_ids = [g['id'] for g in genres]
                        genre_objects = Genre.objects.filter(id__in=genre_ids)
                        game.genres.add(*genre_objects)
                        updated = True

                    # Темы
                    themes = game_data['results'].get('themes', {}).get('items', [])
                    if themes:
                        theme_ids = [t['id'] for t in themes]
                        theme_objects = Theme.objects.filter(id__in=theme_ids)
                        game.themes.add(*theme_objects)
                        updated = True

                    # Перспективы
                    perspectives = game_data['results'].get('perspectives', {}).get('items', [])
                    if perspectives:
                        perspective_ids = [p['id'] for p in perspectives]
                        perspective_objects = PlayerPerspective.objects.filter(id__in=perspective_ids)
                        game.player_perspectives.add(*perspective_objects)
                        updated = True

                    # Режимы игры
                    game_modes = game_data['results'].get('game_modes', {}).get('items', [])
                    if game_modes:
                        mode_ids = [m['id'] for m in game_modes]
                        mode_objects = GameMode.objects.filter(id__in=mode_ids)
                        game.game_modes.add(*mode_objects)
                        updated = True

                    if updated:
                        updated_count += 1

            except Exception as e:
                print(f"⚠️ Ошибка обновления игры {game_data['game_id']}: {e}")
                continue

        # Очищаем батч
        self.games_to_update.clear()

        return updated_count