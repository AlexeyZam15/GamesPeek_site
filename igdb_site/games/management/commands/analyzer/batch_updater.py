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

    def __init__(self, batch_size: int = 100, verbose: bool = False):
        self.batch_size = batch_size  # Сохраняем batch_size
        self.games_to_update: List[Dict] = []
        self.verbose = verbose
        self.progress_bar = None
        self.last_update_time = 0
        self.total_games_added = 0  # Для отладки - общее количество добавленных игр

    def get_optimal_threshold(self):
        """Возвращает оптимальный порог для обновления батча"""
        # Если игр мало - обновляем сразу
        if len(self.games_to_update) < 100:
            return 10

        # Если игр много - используем половину batch_size
        # Но не больше 1000 и не меньше 100
        return min(max(self.batch_size // 2, 100), 1000)

    def get_batch_count(self) -> int:
        """Возвращает текущее количество игр в батче"""
        return len(self.games_to_update)

    def add_game_for_update(self, game_id: int, results: Dict[str, Any], is_keywords: bool) -> int:
        """Добавляет игру для обновления - проверяет дубликаты"""
        # УБРАНО ЛИШНЕЕ СООБЩЕНИЕ

        try:
            # Проверяем, не добавлена ли уже эта игра в текущий батч
            for game_data in self.games_to_update:
                if game_data['game_id'] == game_id:
                    # УБРАНО ЛИШНЕЕ СООБЩЕНИЕ
                    return 0

            # Добавляем игру в батч
            self.games_to_update.append({
                'game_id': game_id,
                'results': results,
                'is_keywords': is_keywords,
            })

            games_in_batch = len(self.games_to_update)

            # УБРАНО ЛИШНЕЕ СООБЩЕНИЕ

            return 1  # Игра добавлена успешно

        except Exception as e:
            # УБРАНО ЛИШНЕЕ СООБЩЕНИЕ
            return 0

    def flush(self) -> int:
        """Обновляет все накопленные игры с детальным логированием"""
        if not self.games_to_update:
            if self.verbose:
                print(f"\nℹ️ Батч пуст, нечего обновлять")
            return 0

        try:
            games_in_batch = len(self.games_to_update)
            print(f"\n💾 Начинаем ОПТИМИЗИРОВАННОЕ обновление батча из {games_in_batch} игр")

            # Разделяем на ключевые слова и обычные критерии
            keyword_games = []
            criteria_games = []

            for game_data in self.games_to_update:
                if game_data['is_keywords']:
                    keyword_games.append(game_data)
                else:
                    criteria_games.append(game_data)

            print(f"   📊 Ключевые слова: {len(keyword_games)} игр")
            print(f"   📊 Обычные критерии: {len(criteria_games)} игр")

            total_updated = 0
            all_skipped = []

            # Обработка ключевых слов
            if keyword_games:
                keywords_updated, keywords_skipped = self._update_keywords_bulk(keyword_games)
                print(f"\n📊 КЛЮЧЕВЫЕ СЛОВА:")
                print(f"   ✅ Обновлено: {keywords_updated} игр")
                print(f"   ⚠️ Пропущено: {len(keyword_games) - keywords_updated} игр")
                if keywords_skipped:
                    for skipped in keywords_skipped[:10]:
                        print(f"      - Игра {skipped['game_id']}: {skipped['reason']}")
                    if len(keywords_skipped) > 10:
                        print(f"      ... и еще {len(keywords_skipped) - 10} игр")
                total_updated += keywords_updated
                all_skipped.extend(keywords_skipped)

            # Обработка критериев
            if criteria_games:
                criteria_updated, criteria_skipped = self._update_criteria_bulk(criteria_games)
                print(f"\n📊 ОБЫЧНЫЕ КРИТЕРИИ:")
                print(f"   ✅ Обновлено: {criteria_updated} игр")
                print(f"   ⚠️ Пропущено: {len(criteria_games) - criteria_updated} игр")
                if criteria_skipped:
                    for skipped in criteria_skipped[:10]:
                        print(f"      - Игра {skipped['game_id']}: {skipped['reason']}")
                    if len(criteria_skipped) > 10:
                        print(f"      ... и еще {len(criteria_skipped) - 10} игр")
                total_updated += criteria_updated
                all_skipped.extend(criteria_skipped)

            print(f"\n📊 РЕЗУЛЬТАТ:")
            print(f"   ✅ Реально обновлено: {total_updated} игр из {games_in_batch}")
            print(f"   ⚠️ Пропущено: {games_in_batch - total_updated} игр")

            # Очищаем батч
            self.games_to_update.clear()

            return total_updated

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при обновлении батча: {e}")
            import traceback
            traceback.print_exc()
            import sys
            sys.exit(1)

    def _update_keywords_bulk(self, keyword_games):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННОЕ обновление ключевых слов
        Возвращает: (обновлено_игр, список_пропущенных_с_причинами)
        """
        from games.models import Game, Keyword
        from django.utils import timezone
        from django.db import transaction
        from collections import defaultdict

        start_time = time.time()

        game_ids = [g['game_id'] for g in keyword_games]

        # 1. Получаем все игры одним запросом
        games = {game.id: game for game in Game.objects.filter(id__in=game_ids)}

        # 2. Собираем все ID ключевых слов из всех игр
        all_keyword_ids = set()
        games_with_keywords = []
        skipped_games = []

        for game_data in keyword_games:
            game = games.get(game_data['game_id'])
            if not game:
                skipped_games.append({
                    'game_id': game_data['game_id'],
                    'reason': 'игра не найдена в БД'
                })
                continue

            keywords_data = game_data['results'].get('keywords', {})
            items = keywords_data.get('items', [])

            if not items:
                skipped_games.append({
                    'game_id': game.id,
                    'reason': 'в результатах нет ключевых слов'
                })
                continue

            keyword_ids = [k['id'] for k in items if 'id' in k]
            if not keyword_ids:
                skipped_games.append({
                    'game_id': game.id,
                    'reason': f'ключевые слова без ID: {items}'
                })
                continue

            all_keyword_ids.update(keyword_ids)
            games_with_keywords.append({
                'game': game,
                'keyword_ids': keyword_ids
            })

        if not games_with_keywords:
            return 0, skipped_games

        # 3. Получаем все существующие ключевые слова
        existing_keywords = set(Keyword.objects.filter(id__in=all_keyword_ids).values_list('id', flat=True))

        missing_keyword_ids = all_keyword_ids - existing_keywords
        if missing_keyword_ids:
            print(
                f"   ⚠️ Предупреждение: {len(missing_keyword_ids)} ключевых слов не найдены в БД: {list(missing_keyword_ids)[:5]}...")

        # 4. Получаем текущие связи всех игр
        through_model = Game.keywords.through

        existing_relations = through_model.objects.filter(
            game_id__in=game_ids,
            keyword_id__in=existing_keywords
        ).values_list('game_id', 'keyword_id')

        existing_by_game = defaultdict(set)
        for game_id, keyword_id in existing_relations:
            existing_by_game[game_id].add(keyword_id)

        # 5. Собираем новые связи
        new_relations = []
        updated_games = set()

        for item in games_with_keywords:
            game = item['game']
            keyword_ids = item['keyword_ids']

            valid_ids = [kid for kid in keyword_ids if kid in existing_keywords]

            if not valid_ids:
                skipped_games.append({
                    'game_id': game.id,
                    'reason': f'все {len(keyword_ids)} ключевых слов не найдены в БД'
                })
                continue

            existing_for_game = existing_by_game.get(game.id, set())
            new_ids = [kid for kid in valid_ids if kid not in existing_for_game]

            if new_ids:
                for kid in new_ids:
                    new_relations.append(
                        through_model(game_id=game.id, keyword_id=kid)
                    )
                updated_games.add(game.id)
            else:
                skipped_games.append({
                    'game_id': game.id,
                    'reason': f'все ключевые слова уже есть у игры (существующих: {len(existing_for_game)})'
                })

        # 6. Массовое добавление
        if new_relations:
            try:
                with transaction.atomic():
                    through_model.objects.bulk_create(new_relations, ignore_conflicts=True)
                    Game.objects.filter(id__in=updated_games).update(updated_at=timezone.now())
            except Exception as e:
                print(f"\n❌ ОШИБКА БД при сохранении ключевых слов: {e}")
                raise

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"⚡ Ключевые слова: {len(updated_games)} игр, {len(new_relations)} связей за {elapsed:.2f}с")

        return len(updated_games), skipped_games

    def _update_criteria_bulk(self, criteria_games):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННОЕ обновление критериев
        Возвращает: (обновлено_игр, список_пропущенных_с_причинами)
        """
        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode
        from django.utils import timezone
        from django.db import transaction
        from collections import defaultdict

        start_time = time.time()

        game_ids = [g['game_id'] for g in criteria_games]

        # 1. Получаем все игры с префетчингом
        games = {game.id: game for game in Game.objects.filter(id__in=game_ids).prefetch_related(
            'genres', 'themes', 'player_perspectives', 'game_modes'
        )}

        criteria_config = [
            ('genres', Genre, 'genre', 'genres'),
            ('themes', Theme, 'theme', 'themes'),
            ('perspectives', PlayerPerspective, 'playerperspective', 'player_perspectives'),
            ('game_modes', GameMode, 'gamemode', 'game_modes')
        ]

        all_new_relations = defaultdict(list)
        updated_games = set()
        skipped_games = []

        for game_data in criteria_games:
            game = games.get(game_data['game_id'])
            if not game:
                skipped_games.append({
                    'game_id': game_data['game_id'],
                    'reason': 'игра не найдена в БД'
                })
                continue

            game_has_updates = False
            game_skipped_reasons = []

            for key, model, field_prefix, relation_field in criteria_config:
                items = game_data['results'].get(key, {}).get('items', [])
                if not items:
                    continue

                item_ids = [item['id'] for item in items if 'id' in item]
                if not item_ids:
                    game_skipped_reasons.append(f'{key}: нет ID в элементах')
                    continue

                # Получаем существующие ID
                existing_ids = set()
                if key == 'genres':
                    existing_ids = set(game.genres.values_list('id', flat=True))
                elif key == 'themes':
                    existing_ids = set(game.themes.values_list('id', flat=True))
                elif key == 'perspectives':
                    existing_ids = set(game.player_perspectives.values_list('id', flat=True))
                elif key == 'game_modes':
                    existing_ids = set(game.game_modes.values_list('id', flat=True))

                # Находим новые ID
                new_ids = [iid for iid in item_ids if iid not in existing_ids]

                if new_ids:
                    # Проверяем, существуют ли эти ID в БД
                    model_objects = model.objects.filter(id__in=new_ids)
                    existing_model_ids = set(model_objects.values_list('id', flat=True))
                    valid_new_ids = [iid for iid in new_ids if iid in existing_model_ids]

                    if valid_new_ids:
                        through_model = getattr(Game, relation_field).through
                        for iid in valid_new_ids:
                            all_new_relations[through_model].append(
                                through_model(game_id=game.id, **{f"{field_prefix}_id": iid})
                            )
                        game_has_updates = True
                    else:
                        game_skipped_reasons.append(f'{key}: новые ID не найдены в БД ({new_ids})')
                else:
                    if item_ids:
                        game_skipped_reasons.append(f'{key}: все {len(item_ids)} элементов уже есть у игры')

            if game_has_updates:
                updated_games.add(game.id)
            elif game_skipped_reasons:
                skipped_games.append({
                    'game_id': game.id,
                    'reason': '; '.join(game_skipped_reasons)
                })
            else:
                skipped_games.append({
                    'game_id': game.id,
                    'reason': 'нет элементов для добавления'
                })

        # 2. Массовое добавление
        total_added = 0
        if all_new_relations:
            try:
                with transaction.atomic():
                    for through_model, relations in all_new_relations.items():
                        if relations:
                            through_model.objects.bulk_create(relations, ignore_conflicts=True)
                            total_added += len(relations)

                    if updated_games:
                        Game.objects.filter(id__in=updated_games).update(updated_at=timezone.now())
            except Exception as e:
                print(f"\n❌ ОШИБКА БД при сохранении критериев: {e}")
                raise

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"⚡ Критерии: {len(updated_games)} игр, {total_added} связей за {elapsed:.2f}с")

        return len(updated_games), skipped_games

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
        """Оптимизированное обновление ключевых слов"""
        from games.models import Game, Keyword
        from django.utils import timezone

        updated_count = 0
        skipped_count = 0
        game_ids = [g['game_id'] for g in keyword_games]

        # Получаем все игры одним запросом с префетчингом ключевых слов
        games_dict = {game.id: game for game in Game.objects.filter(id__in=game_ids).prefetch_related('keywords')}

        # Собираем все ID ключевых слов из всех игр
        all_keyword_ids = []
        games_with_keywords = []

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

        return updated_count

    def _update_criteria_batch(self, criteria_games):
        """Оптимизированное обновление критериев"""
        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode
        from django.utils import timezone

        updated_count = 0
        game_ids = [g['game_id'] for g in criteria_games]

        if self.verbose:
            print(f"\n📊 _update_criteria_batch: обрабатываем {len(criteria_games)} игр")
            print(f"   game_ids: {game_ids}")

        # Получаем все игры одним запросом с префетчингом
        games_dict = {game.id: game for game in Game.objects.filter(id__in=game_ids).prefetch_related(
            'genres', 'themes', 'player_perspectives', 'game_modes'
        )}

        if self.verbose:
            print(f"   найдено игр в БД: {len(games_dict)}")

        for game_data in criteria_games:
            game = games_dict.get(game_data['game_id'])
            if not game:
                if self.verbose:
                    print(f"   ⚠️ Игра {game_data['game_id']} не найдена в БД")
                continue

            if self.verbose:
                print(f"\n   🔍 Обрабатываем игру {game.id}: {game.name}")
                print(f"   Результаты анализа: {game_data['results'].keys()}")

            added_elements = 0

            # Жанры
            genres = game_data['results'].get('genres', {}).get('items', [])
            if self.verbose:
                print(f"   Найдено жанров: {len(genres)}")
            if genres:
                genre_ids = [g['id'] for g in genres]
                existing_genre_ids = set(game.genres.values_list('id', flat=True))
                new_genre_ids = [gid for gid in genre_ids if gid not in existing_genre_ids]

                if new_genre_ids:
                    genre_objects = Genre.objects.filter(id__in=new_genre_ids)
                    if genre_objects.exists():
                        game.genres.add(*genre_objects)
                        added_elements += len(new_genre_ids)
                        if self.verbose:
                            print(f"      ✅ Добавлены жанры: {[g.name for g in genre_objects]}")

            # Темы
            themes = game_data['results'].get('themes', {}).get('items', [])
            if self.verbose:
                print(f"   Найдено тем: {len(themes)}")
            if themes:
                theme_ids = [t['id'] for t in themes]
                existing_theme_ids = set(game.themes.values_list('id', flat=True))
                new_theme_ids = [tid for tid in theme_ids if tid not in existing_theme_ids]

                if new_theme_ids:
                    theme_objects = Theme.objects.filter(id__in=new_theme_ids)
                    if theme_objects.exists():
                        game.themes.add(*theme_objects)
                        added_elements += len(new_theme_ids)
                        if self.verbose:
                            print(f"      ✅ Добавлены темы: {[t.name for t in theme_objects]}")

            # Перспективы
            perspectives = game_data['results'].get('perspectives', {}).get('items', [])
            if self.verbose:
                print(f"   Найдено перспектив: {len(perspectives)}")
            if perspectives:
                perspective_ids = [p['id'] for p in perspectives]
                existing_perspective_ids = set(game.player_perspectives.values_list('id', flat=True))
                new_perspective_ids = [pid for pid in perspective_ids if pid not in existing_perspective_ids]

                if new_perspective_ids:
                    perspective_objects = PlayerPerspective.objects.filter(id__in=new_perspective_ids)
                    if perspective_objects.exists():
                        game.player_perspectives.add(*perspective_objects)
                        added_elements += len(new_perspective_ids)
                        if self.verbose:
                            print(f"      ✅ Добавлены перспективы: {[p.name for p in perspective_objects]}")

            # Режимы игры
            game_modes = game_data['results'].get('game_modes', {}).get('items', [])
            if self.verbose:
                print(f"   Найдено режимов: {len(game_modes)}")
            if game_modes:
                mode_ids = [m['id'] for m in game_modes]
                existing_mode_ids = set(game.game_modes.values_list('id', flat=True))
                new_mode_ids = [mid for mid in mode_ids if mid not in existing_mode_ids]

                if new_mode_ids:
                    mode_objects = GameMode.objects.filter(id__in=new_mode_ids)
                    if mode_objects.exists():
                        game.game_modes.add(*mode_objects)
                        added_elements += len(new_mode_ids)
                        if self.verbose:
                            print(f"      ✅ Добавлены режимы: {[m.name for m in mode_objects]}")

            if added_elements > 0:
                updated_count += 1
                # Обновляем время модификации игры
                game.updated_at = timezone.now()
                game.save(update_fields=['updated_at'])
                if self.verbose:
                    print(f"   ✅ Игра {game.id} обновлена: добавлено {added_elements} элементов")
            else:
                if self.verbose:
                    print(f"   ❌ Игра {game.id} НЕ обновлена: added_elements = {added_elements}")

        if self.verbose:
            print(f"\n📊 _update_criteria_batch вернул: {updated_count} обновленных игр из {len(criteria_games)}")
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