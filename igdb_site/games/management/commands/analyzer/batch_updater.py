# games/management/commands/analyzer/batch_updater.py
"""
Батч-апдейтер для обновления игр с предотвращением рекурсии
"""

from typing import Dict, Any, List
from django.db import transaction
import sys
import time
from .output_manager import UnifiedProgressBar


class BatchUpdater:
    """Батч-апдейтер для обновления игр в базе"""

    BATCH_SIZE_LIMIT = 50  # Максимальный размер батча для обновления

    def __init__(self, batch_size: int = 100, verbose: bool = False):
        self.batch_size = batch_size
        self.games_to_update: List[Dict] = []
        self.verbose = verbose
        self.progress_bar = None
        self.last_update_time = 0
        self.total_games_added = 0  # Для отладки - общее количество добавленных игр

    def get_batch_count(self) -> int:
        """Возвращает текущее количество игр в батче"""
        return len(self.games_to_update)

    def add_game_for_update(self, game_id: int, results: Dict[str, Any], is_keywords: bool) -> int:
        """Добавляет игру для обновления - проверяет дубликаты"""
        try:
            # Проверяем, не добавлена ли уже эта игра в текущий батч
            for game_data in self.games_to_update:
                if game_data['game_id'] == game_id:
                    if self.verbose:
                        print(f"⚠️ Игра {game_id} УЖЕ в батче! Пропускаем повторное добавление.")
                    return 0

            # Добавляем игру в батч
            self.games_to_update.append({
                'game_id': game_id,
                'results': results,
                'is_keywords': is_keywords,
            })

            # Отладочная информация
            games_in_batch = len(self.games_to_update)

            if self.verbose:
                # Показываем информацию о ключевых словах для этой игры
                if is_keywords:
                    keywords_data = results.get('keywords', {})
                    items = keywords_data.get('items', [])
                    if items:
                        print(f"✅ Игра {game_id} добавлена в батч: {len(items)} ключевых слов")
                    else:
                        print(f"✅ Игра {game_id} добавлена в батч (нет ключевых слов)")
                else:
                    print(f"✅ Игра {game_id} добавлена в батч")

                # Показываем общее количество каждые 10 игр
                if games_in_batch % 10 == 0:
                    print(f"📥 Всего игр в батче: {games_in_batch}")

            # ВАЖНО: Если нужно обновить батч, сначала возвращаем 1 (игра добавлена),
            # а flush вызовется отдельно, и статистика обновится после него
            if games_in_batch >= 50:
                if self.verbose:
                    print(f"\n💾 Батч достиг 50 игр, обновляем...")
                # Возвращаем 1 (игра добавлена), flush вызовется отдельно
                return 1

            return 1  # Игра добавлена успешно

        except Exception as e:
            if self.verbose:
                print(f"❌ Ошибка добавления игры {game_id} в батч: {e}")
            return 0

    def flush(self) -> int:
        """Обновляет все накопленные игры - исправленная версия с полными сообщениями"""
        if not self.games_to_update:
            return 0

        try:
            # Отладочная информация
            games_in_batch = len(self.games_to_update)
            if self.verbose:
                print(f"\n💾 Начинаем обновление батча из {games_in_batch} игр")
                print(f"🔄 Режим: {'ключевые слова' if self.games_to_update[0]['is_keywords'] else 'критерии'}")

            from games.models import Game, Keyword
            from django.db import transaction
            from django.utils import timezone

            updated_count = 0
            skipped_count = 0
            game_ids = [g['game_id'] for g in self.games_to_update]

            # Проверяем, есть ли ключевые слова в базе
            keyword_ids_from_batch = []
            for game_data in self.games_to_update:
                if game_data['is_keywords']:
                    keywords_data = game_data['results'].get('keywords', {})
                    items = keywords_data.get('items', [])
                    keyword_ids_from_batch.extend([k['id'] for k in items])

            # Проверяем, существуют ли ключевые слова в базе
            existing_keywords = set()
            if keyword_ids_from_batch:
                existing_keywords = set(
                    Keyword.objects.filter(id__in=keyword_ids_from_batch).values_list('id', flat=True))
                if self.verbose:
                    print(
                        f"🔍 В базе найдено {len(existing_keywords)} ключевых слов из {len(set(keyword_ids_from_batch))} в батче")

            with transaction.atomic():
                # Получаем все игры одним запросом с ключевыми словами
                games = Game.objects.filter(id__in=game_ids).prefetch_related('keywords')
                games_dict = {game.id: game for game in games}

                # Обрабатываем каждую игру в батче
                for i, game_data in enumerate(self.games_to_update, 1):
                    try:
                        game_id = game_data['game_id']
                        game = games_dict.get(game_id)

                        if not game:
                            if self.verbose:
                                print(f"⚠️ Игра {game_id} не найдена в базе")
                            skipped_count += 1
                            continue

                        # Проверяем режим ключевых слов
                        if game_data['is_keywords']:
                            # Обновляем ключевые слова
                            keywords_data = game_data['results'].get('keywords', {})
                            items = keywords_data.get('items', [])

                            if not items:
                                skipped_count += 1
                                continue

                            # Получаем ID ключевых слов
                            keyword_ids = [k['id'] for k in items]

                            # Проверяем, какие ключевые слова существуют в базе
                            valid_keyword_ids = [kid for kid in keyword_ids if kid in existing_keywords]

                            if not valid_keyword_ids:
                                if self.verbose:
                                    print(f"⚠️ Для игры {game_id} нет валидных ключевых слов в базе")
                                skipped_count += 1
                                continue

                            # Получаем существующие ключевые слова у игры
                            existing_game_ids = set(game.keywords.values_list('id', flat=True))

                            # Находим новые ключевые слова
                            new_ids = [kid for kid in valid_keyword_ids if kid not in existing_game_ids]

                            if new_ids:
                                # Получаем объекты Keyword
                                keyword_objects = Keyword.objects.filter(id__in=new_ids)

                                if keyword_objects.exists():
                                    # Добавляем ключевые слова к игре
                                    game.keywords.add(*keyword_objects)
                                    updated_count += 1

                                    if self.verbose:
                                        print(f"✅ Игра {game_id} обновлена: добавлено {len(new_ids)} ключевых слов")

                                    # Показываем прогресс каждые 10 обновленных игр
                                    if updated_count % 10 == 0:
                                        print(f"✅ Обновлено {updated_count} игр...")
                                else:
                                    if self.verbose:
                                        print(f"⚠️ Для игры {game_id} не найдены объекты Keyword по ID: {new_ids}")
                                    skipped_count += 1
                            else:
                                # Все ключевые слова уже есть у игры
                                if self.verbose:
                                    print(f"ℹ️ Игра {game_id} уже имеет все ключевые слова")
                                skipped_count += 1

                            # Обновляем время модификации если были добавлены ключевые слова
                            if new_ids and keyword_objects.exists():
                                game.updated_at = timezone.now()
                                game.save(update_fields=['updated_at'])

                    except Exception as e:
                        if self.verbose:
                            print(f"❌ Ошибка обновления игры {game_data.get('game_id', 'unknown')}: {e}")
                        skipped_count += 1
                        continue

            # ВАЖНО: Очищаем батч
            self.games_to_update.clear()

            # Выводим итоговое сообщение
            if self.verbose:
                print(f"\n📊 Результат: обновлено {updated_count} игр из {games_in_batch} в батче")
                if skipped_count > 0:
                    print(f"ℹ️ Пропущено {skipped_count} игр (уже имеют все ключевые слова или ошибки)")

            return updated_count

        except Exception as e:
            if self.verbose:
                print(f"❌ Критическая ошибка обновления батча: {e}")
                import traceback
                traceback.print_exc()
            # В случае ошибки очищаем батч, чтобы не зациклиться
            self.games_to_update.clear()
            return 0

    def _execute_large_batch_update(self) -> int:
        """Выполняет обновление крупного батча (оптимизировано для скорости)"""
        from games.models import Game, Keyword, Genre, Theme, PlayerPerspective, GameMode
        from django.utils import timezone
        from django.db import transaction

        try:
            with transaction.atomic():
                games_in_batch = len(self.games_to_update)

                # Оптимизированная группировка по типам
                keyword_games = []
                criteria_games = []

                for game_data in self.games_to_update:
                    if game_data['is_keywords']:
                        keyword_games.append(game_data)
                    else:
                        criteria_games.append(game_data)

                updated_count = 0

                # Обработка ключевых слов (если есть)
                if keyword_games:
                    updated_count += self._update_keywords_batch(keyword_games)

                # Обработка критериев (если есть)
                if criteria_games:
                    updated_count += self._update_criteria_batch(criteria_games)

                # Очищаем батч
                self.games_to_update.clear()

                return updated_count

        except Exception as e:
            if self.verbose:
                print(f"⚠️ Ошибка обновления крупного батча: {e}")
            return 0

    def _execute_small_batch_update(self) -> int:
        """Выполняет обновление небольшого батча (оригинальная логика)"""
        return self._execute_batch_update()

    def _update_keywords_batch(self, keyword_games):
        """Оптимизированное обновление ключевых слов - с исправленной логикой"""
        from games.models import Game, Keyword
        from django.utils import timezone

        updated_count = 0
        skipped_count = 0  # Счетчик пропущенных игр
        game_ids = [g['game_id'] for g in keyword_games]

        # Получаем все игры одним запросом с префетчингом ключевых слов
        games_dict = {game.id: game for game in Game.objects.filter(id__in=game_ids).prefetch_related('keywords')}

        # Собираем все ID ключевых слов из всех игр
        all_keyword_ids = []
        games_with_keywords = []  # Игры, у которых действительно есть ключевые слова для добавления

        for game_data in keyword_games:
            keywords_data = game_data['results'].get('keywords', {})
            items = keywords_data.get('items', [])
            if items:
                all_keyword_ids.extend([k['id'] for k in items])
                games_with_keywords.append(game_data)

        if not games_with_keywords:
            if self.verbose:
                print(f"ℹ️ Нет игр с ключевыми словами для обновления")
            return 0

        # Проверяем, какие ключевые слова действительно существуют в базе
        existing_keyword_ids = set(Keyword.objects.filter(id__in=all_keyword_ids).values_list('id', flat=True))

        if self.verbose:
            print(
                f"🔍 В базе найдено {len(existing_keyword_ids)} ключевых слов из {len(set(all_keyword_ids))} запрошенных")

        # Обрабатываем каждую игру с ключевыми словами
        for game_data in games_with_keywords:
            try:
                game = games_dict.get(game_data['game_id'])
                if not game:
                    if self.verbose:
                        print(f"⚠️ Игра {game_data['game_id']} не найдена в базе")
                    skipped_count += 1
                    continue

                keywords_data = game_data['results'].get('keywords', {})
                items = keywords_data.get('items', [])

                # Получаем ID ключевых слов
                keyword_ids = [k['id'] for k in items]

                # Фильтруем только существующие ключевые слова
                valid_keyword_ids = [kid for kid in keyword_ids if kid in existing_keyword_ids]

                if not valid_keyword_ids:
                    if self.verbose:
                        print(f"⚠️ Для игры {game.id} нет валидных ключевых слов в базе")
                    skipped_count += 1
                    continue

                # Получаем существующие ключевые слова у игры
                existing_game_ids = set(game.keywords.values_list('id', flat=True))

                # Находим новые ключевые слова (которых еще нет у игры)
                new_ids = [kid for kid in valid_keyword_ids if kid not in existing_game_ids]

                if not new_ids:
                    # Игра уже имеет все ключевые слова - пропускаем
                    skipped_count += 1
                    if self.verbose:
                        print(f"ℹ️ Игра {game.id} уже имеет все ключевые слова")
                    continue

                # Получаем объекты Keyword
                keyword_objects = Keyword.objects.filter(id__in=new_ids)

                if not keyword_objects.exists():
                    if self.verbose:
                        print(f"⚠️ Для игры {game.id} не найдены объекты Keyword по ID: {new_ids}")
                    skipped_count += 1
                    continue

                # Добавляем ключевые слова к игре
                game.keywords.add(*keyword_objects)
                updated_count += 1

                if self.verbose:
                    print(f"✅ Игра {game.id} обновлена: добавлено {len(new_ids)} ключевых слов")
                elif updated_count % 10 == 0:
                    print(f"✅ Обновлено {updated_count} игр...")

                # Обновляем время модификации
                game.updated_at = timezone.now()
                game.save(update_fields=['updated_at'])

            except Exception as e:
                if self.verbose:
                    print(f"❌ Ошибка обновления игры {game_data.get('game_id', 'unknown')}: {e}")
                skipped_count += 1
                continue

        if self.verbose:
            print(
                f"📊 Обновлено {updated_count} игр, пропущено {skipped_count} (всего {len(games_with_keywords)} с ключевыми словами)")
            if skipped_count > 0:
                print(f"ℹ️ Из них пропущено {skipped_count} игр (уже имеют все ключевые слова)")

        # ВОЗВРАЩАЕМ КОЛИЧЕСТВО ОБНОВЛЕННЫХ ИГР, А НЕ КОЛИЧЕСТВО ЭЛЕМЕНТОВ
        return updated_count

    def _update_criteria_batch(self, criteria_games):
        """Оптимизированное обновление критериев"""
        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode
        from django.utils import timezone

        updated_count = 0
        game_ids = [g['game_id'] for g in criteria_games]

        # Получаем все игры одним запросом с префетчингом
        games_dict = {game.id: game for game in Game.objects.filter(id__in=game_ids).prefetch_related(
            'genres', 'themes', 'player_perspectives', 'game_modes'
        )}

        for game_data in criteria_games:
            game = games_dict.get(game_data['game_id'])
            if not game:
                continue

            added_elements = 0

            # Жанры
            genres = game_data['results'].get('genres', {}).get('items', [])
            if genres:
                genre_ids = [g['id'] for g in genres]
                existing_genre_ids = set(game.genres.values_list('id', flat=True))
                new_genre_ids = [gid for gid in genre_ids if gid not in existing_genre_ids]

                if new_genre_ids:
                    genre_objects = Genre.objects.filter(id__in=new_genre_ids)
                    if genre_objects.exists():
                        game.genres.add(*genre_objects)
                        added_elements += len(new_genre_ids)

            # Темы
            themes = game_data['results'].get('themes', {}).get('items', [])
            if themes:
                theme_ids = [t['id'] for t in themes]
                existing_theme_ids = set(game.themes.values_list('id', flat=True))
                new_theme_ids = [tid for tid in theme_ids if tid not in existing_theme_ids]

                if new_theme_ids:
                    theme_objects = Theme.objects.filter(id__in=new_theme_ids)
                    if theme_objects.exists():
                        game.themes.add(*theme_objects)
                        added_elements += len(new_theme_ids)

            # Перспективы
            perspectives = game_data['results'].get('perspectives', {}).get('items', [])
            if perspectives:
                perspective_ids = [p['id'] for p in perspectives]
                existing_perspective_ids = set(game.player_perspectives.values_list('id', flat=True))
                new_perspective_ids = [pid for pid in perspective_ids if pid not in existing_perspective_ids]

                if new_perspective_ids:
                    perspective_objects = PlayerPerspective.objects.filter(id__in=new_perspective_ids)
                    if perspective_objects.exists():
                        game.player_perspectives.add(*perspective_objects)
                        added_elements += len(new_perspective_ids)

            # Режимы игры
            game_modes = game_data['results'].get('game_modes', {}).get('items', [])
            if game_modes:
                mode_ids = [m['id'] for m in game_modes]
                existing_mode_ids = set(game.game_modes.values_list('id', flat=True))
                new_mode_ids = [mid for mid in mode_ids if mid not in existing_mode_ids]

                if new_mode_ids:
                    mode_objects = GameMode.objects.filter(id__in=new_mode_ids)
                    if mode_objects.exists():
                        game.game_modes.add(*mode_objects)
                        added_elements += len(new_mode_ids)

            if added_elements > 0:
                updated_count += 1

        return updated_count

    @transaction.atomic
    def _execute_batch_update(self) -> int:
        """Выполняет обновление накопленного батча"""
        if not self.games_to_update:
            return 0

        from games.models import Game, Keyword, Genre, Theme, PlayerPerspective, GameMode
        from django.utils import timezone

        # Запоминаем сколько игр в батче
        games_in_batch = len(self.games_to_update)

        # Инициализируем прогресс-бар для обновлений
        if not self.verbose:
            self.progress_bar = UnifiedProgressBar(
                total=games_in_batch,
                desc="Обновление батча",
                bar_length=30,
                update_interval=0.1,
                stat_width=5,
                emoji_spacing=1,
                is_batch=True
            )

        updated_games_count = 0
        updated_elements_count = 0

        # Получаем все игры одним запросом
        game_ids = [game_data['game_id'] for game_data in self.games_to_update]
        games_dict = {game.id: game for game in Game.objects.filter(id__in=game_ids).prefetch_related(
            'keywords', 'genres', 'themes', 'player_perspectives', 'game_modes'
        )}

        for i, game_data in enumerate(self.games_to_update):
            try:
                # Обновляем прогресс-бар обновлений
                if self.progress_bar:
                    current_time = time.time()
                    if current_time - self.last_update_time > 0.1 or i == 0:
                        self.progress_bar.update(1)
                        self.last_update_time = current_time

                game = games_dict.get(game_data['game_id'])
                if not game:
                    continue

                added_elements = 0

                if game_data['is_keywords']:
                    keywords = game_data['results'].get('keywords', {}).get('items', [])
                    if keywords:
                        keyword_ids = [k['id'] for k in keywords]
                        existing_keyword_ids = set(game.keywords.values_list('id', flat=True))
                        new_keyword_ids = [kid for kid in keyword_ids if kid not in existing_keyword_ids]

                        if new_keyword_ids:
                            keyword_objects = Keyword.objects.filter(id__in=new_keyword_ids)
                            if keyword_objects.exists():
                                game.keywords.add(*keyword_objects)
                                added_elements += len(new_keyword_ids)
                else:
                    # Жанры
                    genres = game_data['results'].get('genres', {}).get('items', [])
                    if genres:
                        genre_ids = [g['id'] for g in genres]
                        existing_genre_ids = set(game.genres.values_list('id', flat=True))
                        new_genre_ids = [gid for gid in genre_ids if gid not in existing_genre_ids]

                        if new_genre_ids:
                            genre_objects = Genre.objects.filter(id__in=new_genre_ids)
                            if genre_objects.exists():
                                game.genres.add(*genre_objects)
                                added_elements += len(new_genre_ids)

                    # Темы
                    themes = game_data['results'].get('themes', {}).get('items', [])
                    if themes:
                        theme_ids = [t['id'] for t in themes]
                        existing_theme_ids = set(game.themes.values_list('id', flat=True))
                        new_theme_ids = [tid for tid in theme_ids if tid not in existing_theme_ids]

                        if new_theme_ids:
                            theme_objects = Theme.objects.filter(id__in=new_theme_ids)
                            if theme_objects.exists():
                                game.themes.add(*theme_objects)
                                added_elements += len(new_theme_ids)

                    # Перспективы
                    perspectives = game_data['results'].get('perspectives', {}).get('items', [])
                    if perspectives:
                        perspective_ids = [p['id'] for p in perspectives]
                        existing_perspective_ids = set(game.player_perspectives.values_list('id', flat=True))
                        new_perspective_ids = [pid for pid in perspective_ids if pid not in existing_perspective_ids]

                        if new_perspective_ids:
                            perspective_objects = PlayerPerspective.objects.filter(id__in=new_perspective_ids)
                            if perspective_objects.exists():
                                game.player_perspectives.add(*perspective_objects)
                                added_elements += len(new_perspective_ids)

                    # Режимы игры
                    game_modes = game_data['results'].get('game_modes', {}).get('items', [])
                    if game_modes:
                        mode_ids = [m['id'] for m in game_modes]
                        existing_mode_ids = set(game.game_modes.values_list('id', flat=True))
                        new_mode_ids = [mid for mid in mode_ids if mid not in existing_mode_ids]

                        if new_mode_ids:
                            mode_objects = GameMode.objects.filter(id__in=new_mode_ids)
                            if mode_objects.exists():
                                game.game_modes.add(*mode_objects)
                                added_elements += len(new_mode_ids)

                if added_elements > 0:
                    updated_games_count += 1
                    updated_elements_count += added_elements
                    game._needs_save = True

            except Exception:
                pass

        # Batch save всех измененных игр
        games_to_save = [game for game in games_dict.values() if hasattr(game, '_needs_save') and game._needs_save]
        if games_to_save:
            # Обновляем поле updated_at на текущее время
            current_time = timezone.now()
            for game in games_to_save:
                game.updated_at = current_time

            # Массово сохраняем игры с обновленным полем updated_at
            Game.objects.bulk_update(games_to_save, ['updated_at'])

        # Завершаем прогресс-бар обновлений
        if self.progress_bar:
            final_message = None
            if updated_games_count > 0:
                final_message = f"💾 Обновлено {updated_games_count} игр ({updated_elements_count} элементов)"
            else:
                final_message = "💾 Игры не нуждались в обновлении"

            self.progress_bar.finish(final_message)
            self.progress_bar = None

        # Очищаем батч
        self.games_to_update.clear()

        return updated_games_count
