from games.igdb_api import make_igdb_request
from games.models import Game


class TacticalRpgLoader:
    """Класс для загрузки тактических RPG"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def load_tactical_rpg_games(self, debug=False, limit=0, offset=0, min_rating_count=0, skip_existing=True):
        """Загрузка тактических RPG по жанру и ключевым словам"""
        self.stdout.write('🔍 Поиск тактических RPG...')

        if limit > 0:
            self.stdout.write(f'   🔒 Установлен лимит: {limit} игр')
        if offset > 0:
            self.stdout.write(f'   ⏭️  Пропуск первых: {offset} игр')
        if min_rating_count > 0:
            self.stdout.write(f'   ⭐ Минимальное количество оценок: {min_rating_count}')
        if skip_existing:
            self.stdout.write(f'   ⏭️  Режим skip-existing: пропуск игр, которые уже есть в базе')

        if debug:
            self.stdout.write('   🔎 Поиск жанра "Tactical"...')

        # Ищем жанр "Tactical"
        genre_query = 'fields id,name; where name = "Tactical";'
        tactical_genres = make_igdb_request('genres', genre_query, debug=False)
        tactical_genre_id = tactical_genres[0]['id'] if tactical_genres else None

        if debug:
            if tactical_genre_id:
                self.stdout.write(f'   ✅ Жанр Tactical найден: ID {tactical_genre_id}')
            else:
                self.stdout.write('   ❌ Жанр Tactical не найден')

        if debug:
            self.stdout.write('   🔎 Поиск ключевого слова "tactical turn-based combat"...')

        # Ищем ключевое слово
        keyword_query = 'fields id,name; where name = "tactical turn-based combat";'
        tactical_keywords = make_igdb_request('keywords', keyword_query, debug=False)
        tactical_keyword_id = tactical_keywords[0]['id'] if tactical_keywords else None

        if debug:
            if tactical_keyword_id:
                self.stdout.write(f'   ✅ Ключевое слово найдено: ID {tactical_keyword_id}')
            else:
                self.stdout.write('   ❌ Ключевое слово не найдено')

        if not tactical_genre_id and not tactical_keyword_id:
            self.stdout.write('❌ Не найдены тактический жанр или ключевое слово')
            return []

        where_conditions = []
        if tactical_genre_id:
            where_conditions.append(f'genres = ({tactical_genre_id})')
        if tactical_keyword_id:
            where_conditions.append(f'keywords = ({tactical_keyword_id})')

        where_clause = ' | '.join(where_conditions)
        full_where = f'genres = (12) & ({where_clause})'  # 12 = RPG жанр

        if min_rating_count > 0:
            full_where = f'{full_where} & rating_count >= {min_rating_count}'

        if debug:
            self.stdout.write('   🎯 Построение запроса...')
            self.stdout.write(f'   📋 Условие: {full_where}')

        from .data_collector import DataCollector
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_games_by_query(full_where, debug, limit, offset, skip_existing)