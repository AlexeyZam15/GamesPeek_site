from django.db import models
from django.utils.translation import gettext_lazy as _
from django.utils import timezone


# ===== GAME TYPE ENUMS =====
class GameTypeEnum:
    """Enum for predefined game types from IGDB"""
    # Main games
    MAIN_GAME = 0
    STANDALONE_EXPANSION = 4
    SEASON = 7
    REMAKE = 8
    REMASTER = 9
    EXPANDED_GAME = 10

    # Non-main games
    DLC_ADDON = 1
    EXPANSION = 2
    BUNDLE = 3
    MOD = 5
    EPISODE = 6
    PORT = 11
    FORK = 12
    PACK_ADDON = 13
    UPDATE = 14

    # Django choices
    CHOICES = [
        # Main games
        (MAIN_GAME, _('Main game')),
        (STANDALONE_EXPANSION, _('Standalone expansion')),
        (SEASON, _('Season')),
        (REMAKE, _('Remake')),
        (REMASTER, _('Remaster')),
        (EXPANDED_GAME, _('Expanded game')),

        # Non-main games
        (DLC_ADDON, _('DLC/Addon')),
        (EXPANSION, _('Expansion')),
        (BUNDLE, _('Bundle')),
        (MOD, _('Mod')),
        (EPISODE, _('Episode')),
        (PORT, _('Port')),
        (FORK, _('Fork')),
        (PACK_ADDON, _('Pack / Addon')),
        (UPDATE, _('Update')),
    ]

    # Type information dictionary
    TYPE_INFO = {
        # Main games
        MAIN_GAME: {
            'name': 'main_game',
            'is_primary': True,
            'flag': None,
        },
        STANDALONE_EXPANSION: {
            'name': 'standalone_expansion',
            'is_primary': True,
            'flag': 'is_standalone_expansion',
        },
        SEASON: {
            'name': 'season',
            'is_primary': True,
            'flag': 'is_season',
        },
        REMAKE: {
            'name': 'remake',
            'is_primary': True,
            'flag': 'is_remake',
        },
        REMASTER: {
            'name': 'remaster',
            'is_primary': True,
            'flag': 'is_remaster',
        },
        EXPANDED_GAME: {
            'name': 'expanded_game',
            'is_primary': True,
            'flag': 'is_expanded_game',
        },

        # Non-main games
        DLC_ADDON: {
            'name': 'dlc_addon',
            'is_primary': False,
            'flag': 'is_dlc',
        },
        EXPANSION: {
            'name': 'expansion',
            'is_primary': False,
            'flag': 'is_expansion',
        },
        BUNDLE: {
            'name': 'bundle',
            'is_primary': False,
            'flag': 'is_bundle_component',
        },
        MOD: {
            'name': 'mod',
            'is_primary': False,
            'flag': 'is_mod',
        },
        EPISODE: {
            'name': 'episode',
            'is_primary': False,
            'flag': 'is_episode',
        },
        PORT: {
            'name': 'port',
            'is_primary': False,
            'flag': 'is_port',
        },
        FORK: {
            'name': 'fork',
            'is_primary': False,
            'flag': 'is_fork',
        },
        PACK_ADDON: {
            'name': 'pack_addon',
            'is_primary': False,
            'flag': 'is_pack_addon',
        },
        UPDATE: {
            'name': 'update',
            'is_primary': False,
            'flag': 'is_update',
        },
    }

    # Lists for convenience
    PRIMARY_GAME_TYPES = [
        MAIN_GAME, STANDALONE_EXPANSION, SEASON,
        REMAKE, REMASTER, EXPANDED_GAME
    ]

    NON_PRIMARY_GAME_TYPES = [
        DLC_ADDON, EXPANSION, BUNDLE, MOD, EPISODE,
        PORT, FORK, PACK_ADDON, UPDATE
    ]

    # Name to ID mapping
    NAME_TO_ID = {
        'main_game': MAIN_GAME,
        'standalone_expansion': STANDALONE_EXPANSION,
        'season': SEASON,
        'remake': REMAKE,
        'remaster': REMASTER,
        'expanded_game': EXPANDED_GAME,
        'dlc_addon': DLC_ADDON,
        'expansion': EXPANSION,
        'bundle': BUNDLE,
        'mod': MOD,
        'episode': EPISODE,
        'port': PORT,
        'fork': FORK,
        'pack_addon': PACK_ADDON,
        'update': UPDATE,
    }

    @classmethod
    def get_type_info(cls, game_type_id):
        """Get type information by ID"""
        if game_type_id in cls.TYPE_INFO:
            return cls.TYPE_INFO[game_type_id]
        return {
            'name': f'unknown_{game_type_id}',
            'is_primary': False,
            'flag': None,
        }

    @classmethod
    def get_name(cls, game_type_id):
        """Get type name by ID"""
        info = cls.get_type_info(game_type_id)
        return info['name']

    @classmethod
    def is_primary(cls, game_type_id):
        """Check if game type is primary"""
        info = cls.get_type_info(game_type_id)
        return info['is_primary']

    @classmethod
    def get_id_by_name(cls, type_name):
        """Get ID by type name"""
        return cls.NAME_TO_ID.get(type_name)

    @classmethod
    def get_all_flags(cls):
        """Get all flag names"""
        flags = set()
        for config in cls.TYPE_INFO.values():
            if config['flag']:
                flags.add(config['flag'])
        return list(flags)


# ===== GAME MANAGER =====
class GameManager(models.Manager):
    """Custom manager for Game model with game type filters"""

    def bulk_update_wiki_descriptions(self, descriptions_dict, batch_size=1000):
        """
        Массовое обновление wiki описаний
        descriptions_dict: {game_id: description}
        """
        from django.db import transaction
        from django.db.models import Case, When, Value

        if not descriptions_dict:
            return 0

        game_ids = list(descriptions_dict.keys())
        updated_count = 0

        # Разбиваем на батчи
        for i in range(0, len(game_ids), batch_size):
            batch_ids = game_ids[i:i + batch_size]

            # Создаем CASE выражения
            whens = []
            for game_id in batch_ids:
                description = descriptions_dict.get(game_id)
                if description is not None:  # Может быть пустой строкой
                    whens.append(When(id=game_id, then=Value(description)))

            if whens:
                with transaction.atomic():
                    result = self.filter(id__in=batch_ids).update(
                        wiki_description=Case(*whens, default='wiki_description')
                    )
                    updated_count += result

            # Небольшая пауза между батчами
            import time
            if i + batch_size < len(game_ids):
                time.sleep(0.1)

        return updated_count

    def get_games_for_wiki_import(self, filters=None, limit=None, offset=0):
        """
        Получить игры для импорта с фильтрами
        filters: dict с фильтрами
        """
        queryset = self.all()

        if filters:
            # Применяем фильтры
            if filters.get('empty_wiki'):
                queryset = queryset.filter(wiki_description__isnull=True)
            if filters.get('empty_all'):
                queryset = queryset.filter(
                    wiki_description__isnull=True,
                    rawg_description__isnull=True,
                    summary__isnull=True
                )
            if filters.get('has_summary'):
                queryset = queryset.filter(summary__isnull=False)

        # Сортировка для предсказуемого порядка
        queryset = queryset.order_by('id')

        # Применяем лимит и offset
        if limit:
            queryset = queryset[offset:offset + limit]

        return queryset.values('id', 'name')

    def count_games_without_wiki(self):
        """Количество игр без wiki описания"""
        return self.filter(wiki_description__isnull=True).count()

    def get_chunked_games(self, chunk_size=100, filters=None):
        """Генератор для получения игр порциями"""
        offset = 0
        total_processed = 0

        while True:
            games_chunk = list(self.get_games_for_wiki_import(
                filters=filters,
                limit=chunk_size,
                offset=offset
            ))

            if not games_chunk:
                break

            yield games_chunk
            offset += chunk_size
            total_processed += len(games_chunk)

            # Для отладки
            if total_processed % 1000 == 0:
                print(f"Подготовлено {total_processed} игр...")

    def get_games_without_wiki(self, limit=None):
        """Получить игры без wiki описания"""
        queryset = self.filter(wiki_description__isnull=True)
        if limit:
            queryset = queryset[:limit]
        return queryset

    def get_all_for_wiki_import(self, chunk_size=100):
        """Генератор для получения игр порциями"""
        games = self.all().order_by('id').values('id', 'name')
        for i in range(0, games.count(), chunk_size):
            yield games[i:i + chunk_size]

    def primary(self):
        """Get primary games"""
        return self.filter(game_type__in=GameTypeEnum.PRIMARY_GAME_TYPES)

    def non_primary(self):
        """Get non-primary games"""
        return self.filter(game_type__in=GameTypeEnum.NON_PRIMARY_GAME_TYPES)

    def by_type(self, type_name):
        """Get games by type name"""
        type_id = GameTypeEnum.get_id_by_name(type_name)
        if type_id is not None:
            return self.filter(game_type=type_id)
        return self.none()

    def main_games(self):
        """Get only main games"""
        return self.filter(game_type=GameTypeEnum.MAIN_GAME)

    def dlc_games(self):
        """Get only DLC games"""
        return self.filter(game_type=GameTypeEnum.DLC_ADDON)

    def expansions(self):
        """Get only expansions"""
        return self.filter(game_type=GameTypeEnum.EXPANSION)

    def standalone_expansions(self):
        """Get only standalone expansions"""
        return self.filter(game_type=GameTypeEnum.STANDALONE_EXPANSION)

    def remakes(self):
        """Get only remakes"""
        return self.filter(game_type=GameTypeEnum.REMAKE)

    def remasters(self):
        """Get only remasters"""
        return self.filter(game_type=GameTypeEnum.REMASTER)

    def bundles(self):
        """Get only bundles"""
        return self.filter(game_type=GameTypeEnum.BUNDLE)

    def mods(self):
        """Get only mods"""
        return self.filter(game_type=GameTypeEnum.MOD)

    def episodes(self):
        """Get only episodes"""
        return self.filter(game_type=GameTypeEnum.EPISODE)

    def seasons(self):
        """Get only seasons"""
        return self.filter(game_type=GameTypeEnum.SEASON)

    def ports(self):
        """Get only ports"""
        return self.filter(game_type=GameTypeEnum.PORT)

    def forks(self):
        """Get only forks"""
        return self.filter(game_type=GameTypeEnum.FORK)

    def pack_addons(self):
        """Get only pack addons"""
        return self.filter(game_type=GameTypeEnum.PACK_ADDON)

    def updates(self):
        """Get only updates"""
        return self.filter(game_type=GameTypeEnum.UPDATE)


# ===== GAME MODEL =====
class Game(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    summary = models.TextField(blank=True, null=True)

    # ТОЛЬКО ЭТО ПОЛЕ ДОЛЖНО БЫТЬ - game_type как IntegerField
    game_type = models.IntegerField(
        choices=GameTypeEnum.CHOICES,
        null=True,
        blank=True,
        help_text="Game type from IGDB"
    )

    # Relationships with other games
    parent_game = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='child_games',
        help_text="Parent game (for DLC, expansions, etc.)"
    )

    version_parent = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='version_children',
        help_text="Version parent (base version of the game)"
    )

    version_title = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Version title"
    )

    rawg_description = models.TextField(
        blank=True,
        null=True,
        help_text="Game description imported from RAWG.io",
        verbose_name="Description (RAWG)"
    )

    storyline = models.TextField(blank=True, null=True)
    rating = models.FloatField(blank=True, null=True)
    rating_count = models.IntegerField(default=0)
    first_release_date = models.DateTimeField(blank=True, null=True)

    # Many-to-many relationships
    genres = models.ManyToManyField('Genre', blank=True)
    platforms = models.ManyToManyField('Platform', blank=True)
    keywords = models.ManyToManyField('Keyword', blank=True)

    # Series relationships
    series = models.ManyToManyField(
        'Series',
        blank=True,
        related_name='games'
    )

    series_order = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Order in series (1, 2, 3...)"
    )

    # Company relationships
    developers = models.ManyToManyField(
        'Company',
        related_name='developed_games',
        blank=True
    )
    publishers = models.ManyToManyField(
        'Company',
        related_name='published_games',
        blank=True
    )

    # Additional categories
    themes = models.ManyToManyField('Theme', blank=True)
    player_perspectives = models.ManyToManyField('PlayerPerspective', blank=True)
    game_modes = models.ManyToManyField('GameMode', blank=True)

    cover_url = models.URLField(blank=True, null=True)

    # В классе Game (в конце полей, перед objects = GameManager())
    # В классе Game, после rawg_description добавьте:
    wiki_description = models.TextField(
        blank=True,
        null=True,
        help_text="Game description from Wikipedia (Gameplay section)",
        verbose_name="Description (Wikipedia)"
    )

    # Также обновим индекс для поиска по описаниям:
    # В class Meta в indexes измените последний индекс:
    models.Index(fields=['summary', 'storyline', 'rawg_description', 'wiki_description']),

    # Custom manager
    objects = GameManager()

    class Meta:
        ordering = ['-rating_count']
        indexes = [
            models.Index(fields=['-rating_count']),
            models.Index(fields=['-rating']),
            models.Index(fields=['name']),
            models.Index(fields=['-first_release_date']),
            models.Index(fields=['igdb_id']),
            models.Index(fields=['game_type']),
            models.Index(fields=['id', 'name']),
            # Обновленный индекс включая wiki_description:
            models.Index(fields=['summary', 'storyline', 'rawg_description', 'wiki_description']),
        ]

    # ===== GAME TYPE PROPERTIES =====
    @property
    def game_type_info(self):
        """Get complete game type information"""
        return GameTypeEnum.get_type_info(self.game_type)

    @property
    def game_type_name(self):
        """Get game type name"""
        return GameTypeEnum.get_name(self.game_type)

    @property
    def is_primary_game(self):
        """Check if this is a primary game"""
        return GameTypeEnum.is_primary(self.game_type)

    @property
    def game_type_flag(self):
        """Get game type flag name"""
        info = self.game_type_info
        return info.get('flag')

    # Individual type check properties
    @property
    def is_main_game(self):
        return self.game_type == GameTypeEnum.MAIN_GAME

    @property
    def is_standalone_expansion(self):
        return self.game_type == GameTypeEnum.STANDALONE_EXPANSION

    @property
    def is_season(self):
        return self.game_type == GameTypeEnum.SEASON

    @property
    def is_remake(self):
        return self.game_type == GameTypeEnum.REMAKE

    @property
    def is_remaster(self):
        return self.game_type == GameTypeEnum.REMASTER

    @property
    def is_expanded_game(self):
        return self.game_type == GameTypeEnum.EXPANDED_GAME

    @property
    def is_dlc(self):
        return self.game_type == GameTypeEnum.DLC_ADDON

    @property
    def is_expansion(self):
        return self.game_type == GameTypeEnum.EXPANSION

    @property
    def is_bundle_component(self):
        return self.game_type == GameTypeEnum.BUNDLE

    @property
    def is_mod(self):
        return self.game_type == GameTypeEnum.MOD

    @property
    def is_episode(self):
        return self.game_type == GameTypeEnum.EPISODE

    @property
    def is_port(self):
        return self.game_type == GameTypeEnum.PORT

    @property
    def is_fork(self):
        return self.game_type == GameTypeEnum.FORK

    @property
    def is_pack_addon(self):
        return self.game_type == GameTypeEnum.PACK_ADDON

    @property
    def is_update(self):
        return self.game_type == GameTypeEnum.UPDATE

    # ===== SERIES PROPERTIES =====
    @property
    def is_part_of_series(self):
        """Check if game belongs to any series"""
        return self.series.exists()

    @property
    def main_series(self):
        """Get first (main) series of the game"""
        return self.series.first()

    @property
    def display_series_info(self):
        """Display information about series"""
        if not self.series.exists():
            return ""

        series_names = list(self.series.values_list('name', flat=True))
        if len(series_names) == 1:
            info = series_names[0]
            if self.series_order:
                info += f" (Part {self.series_order})"
            return info
        else:
            info = ", ".join(series_names)
            if self.series_order:
                info += f" (Main series: Part {self.series_order})"
            return f"Series: {info}"

    def get_series_games(self, series=None):
        """Get all games from the same series/series"""
        if series:
            # If specific series is specified
            return series.games.exclude(id=self.id).order_by('first_release_date')
        elif self.series.exists():
            # If no series specified, take first (main) series
            main_series = self.series.first()
            return main_series.games.exclude(id=self.id).order_by('first_release_date')
        return Game.objects.none()

    def get_all_series_games(self):
        """Get all games from all series of this game"""
        all_games = Game.objects.none()
        for series in self.series.all():
            series_games = series.games.exclude(id=self.id)
            all_games = all_games.union(series_games)
        return all_games.distinct()

    @property
    def series_count(self):
        """Number of series the game belongs to"""
        return self.series.count()

    # ===== COMPANY PROPERTIES =====
    @property
    def main_developer(self):
        """Main developer (first in list)"""
        return self.developers.first()

    @property
    def main_publisher(self):
        """Main publisher (first in list)"""
        return self.publishers.first()

    @property
    def developer_names(self):
        """List of developer names"""
        return list(self.developers.values_list('name', flat=True))

    @property
    def publisher_names(self):
        """List of publisher names"""
        return list(self.publishers.values_list('name', flat=True))

    # ===== THEME & PERSPECTIVE PROPERTIES =====
    @property
    def theme_names(self):
        """List of themes"""
        return list(self.themes.values_list('name', flat=True))

    @property
    def perspective_names(self):
        """List of perspectives"""
        return list(self.player_perspectives.values_list('name', flat=True))

    @property
    def game_mode_names(self):
        """List of game modes"""
        return list(self.game_modes.values_list('name', flat=True))

    # ===== KEYWORD CATEGORY PROPERTIES =====
    @property
    def gameplay_keywords(self):
        return self.keywords.filter(category__name='Gameplay')

    @property
    def setting_keywords(self):
        """Get only setting keywords"""
        return self.keywords.filter(category__name='Setting')

    @property
    def genre_keywords(self):
        """Get only genre keywords"""
        return self.keywords.filter(category__name='Genre')

    @property
    def narrative_keywords(self):
        """Get only narrative keywords"""
        return self.keywords.filter(category__name='Narrative')

    @property
    def character_keywords(self):
        """Get only character keywords"""
        return self.keywords.filter(category__name='Characters')

    @property
    def technical_keywords(self):
        """Get only technical keywords"""
        return self.keywords.filter(category__name='Technical')

    @property
    def graphics_keywords(self):
        """Get only graphics keywords"""
        return self.keywords.filter(category__name='Graphics')

    @property
    def platform_keywords(self):
        """Get only platform keywords"""
        return self.keywords.filter(category__name='Platform')

    @property
    def multiplayer_keywords(self):
        """Get only multiplayer keywords"""
        return self.keywords.filter(category__name='Multiplayer')

    @property
    def achievement_keywords(self):
        """Get only achievement keywords"""
        return self.keywords.filter(category__name='Achievements')

    @property
    def audio_keywords(self):
        """Get only audio keywords"""
        return self.keywords.filter(category__name='Audio')

    @property
    def context_keywords(self):
        """Get only context keywords"""
        return self.keywords.filter(category__name='Context')

    @property
    def development_keywords(self):
        """Get only development keywords"""
        return self.keywords.filter(category__name='Development')

    def get_keywords_by_category(self, category_name):
        """Universal method to get keywords by category"""
        return self.keywords.filter(category__name=category_name)

    # ===== DISPLAY METHODS =====
    def get_game_type_display(self):
        """Get display name for game type"""
        if self.game_type is None:
            return "No game type"

        # Use Django's get_FOO_display() method
        for choice_id, choice_name in GameTypeEnum.CHOICES:
            if choice_id == self.game_type:
                return str(choice_name)
        return f"Unknown ({self.game_type})"

    def get_full_title(self):
        """Get full title with version info"""
        title = self.name
        if self.version_title:
            title += f" - {self.version_title}"
        return title

    def __str__(self):
        type_suffix = ""
        if self.game_type is not None:
            type_suffix = f" [{self.get_game_type_display()}]"
        return f"{self.name}{type_suffix}"


# ===== REMAINING MODELS (unchanged except removal) =====
class GameSimilarityDetail(models.Model):
    """Detailed cache for pre-calculated game similarity"""
    source_game = models.ForeignKey('Game', on_delete=models.CASCADE, related_name='similarity_details_as_source')
    target_game = models.ForeignKey('Game', on_delete=models.CASCADE, related_name='similarity_details_as_target')

    # Common elements
    common_genres = models.IntegerField(default=0)
    common_keywords = models.IntegerField(default=0)
    common_themes = models.IntegerField(default=0)
    common_developers = models.IntegerField(default=0)
    common_perspectives = models.IntegerField(default=0)
    common_game_modes = models.IntegerField(default=0)

    # Calculated similarity
    calculated_similarity = models.FloatField(default=0.0)

    # Metadata
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['source_game', 'target_game']
        indexes = [
            models.Index(fields=['source_game', 'calculated_similarity']),
            models.Index(fields=['calculated_similarity']),
        ]
        verbose_name = "Game similarity detail"
        verbose_name_plural = "Game similarity details"

    def __str__(self):
        return f"{self.source_game.name} -> {self.target_game.name}: {self.calculated_similarity:.1f}%"


class GameCountsCache(models.Model):
    """Model for caching game element counts"""
    game = models.OneToOneField('Game', on_delete=models.CASCADE, related_name='counts_cache')

    # Element counts
    genres_count = models.IntegerField(default=0)
    keywords_count = models.IntegerField(default=0)
    themes_count = models.IntegerField(default=0)
    developers_count = models.IntegerField(default=0)
    perspectives_count = models.IntegerField(default=0)
    game_modes_count = models.IntegerField(default=0)

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Counts for {self.game.name}"


class Company(models.Model):
    """Company (developer/publisher)"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    website = models.URLField(blank=True, max_length=500)

    # Logo field for IGDB
    logo_igdb_id = models.CharField(max_length=50, blank=True, null=True, help_text="Logo ID in IGDB")
    logo_url = models.URLField(blank=True, max_length=500)
    start_date = models.DateTimeField(null=True, blank=True)
    changed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Company'
        verbose_name_plural = 'Companies'
        ordering = ['name']

    def __str__(self):
        return self.name

    @property
    def logo_image_url(self):
        """Generate URL for logo from IGDB if logo_igdb_id exists"""
        if self.logo_igdb_id:
            return f"https://images.igdb.com/igdb/image/upload/t_thumb/{self.logo_igdb_id}.jpg"
        elif self.logo_url:
            return self.logo_url
        return ""


class Series(models.Model):
    """Игровые серии (например, The Legend of Zelda, Final Fantasy)"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # Дополнительные поля для лучшей организации
    slug = models.SlugField(max_length=255, unique=True, blank=True, null=True)
    is_main_series = models.BooleanField(default=True, help_text="Основная серия (не спин-офф)")
    parent_series = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='subseries',
        help_text="Родительская серия, если это подсерия"
    )

    def __str__(self):
        return self.name

    @property
    def game_count(self):
        """Количество игр в серии"""
        return self.games.count()

    @property
    def first_game(self):
        """Первая игра в серии (по дате релиза или порядковому номеру)"""
        return self.games.order_by('first_release_date', 'series_order').first()

    @property
    def latest_game(self):
        """Последняя игра в серии"""
        return self.games.order_by('-first_release_date').first()

    @property
    def years_active(self):
        """Годы активности серии"""
        games = self.games.exclude(first_release_date__isnull=True)
        if games.exists():
            dates = games.values_list('first_release_date', flat=True)
            years = [date.year for date in dates if date]
            if years:
                return f"{min(years)} - {max(years)}"
        return "N/A"

    def get_games_in_order(self):
        """Возвращает игры в правильном порядке"""
        # Сначала по порядковому номеру, потом по дате релиза
        return self.games.order_by('series_order', 'first_release_date')

    class Meta:
        verbose_name_plural = "Series"
        ordering = ['name']


class Theme(models.Model):
    """Темы игр (например, Fantasy, Horror, Sci-Fi)"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']


class PlayerPerspective(models.Model):
    """Перспектива игрока (например, First-person, Third-person)"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']


class GameMode(models.Model):
    """Режимы игры (например, Single-player, Multiplayer)"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name']


class KeywordCategory(models.Model):
    """Категория ключевых слов (геймплей, сеттинг, etc.)"""
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name_plural = "Keyword Categories"


class Keyword(models.Model):
    """Базовая модель ключевых слов из IGDB"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)
    category = models.ForeignKey(
        KeywordCategory,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='keywords'
    )

    cached_usage_count = models.IntegerField(default=0)
    last_count_update = models.DateTimeField(null=True, blank=True)

    # ... существующие методы ...

    def update_cached_count(self):
        """Обновляет кэшированное значение"""
        from .models import Game
        actual_count = Game.objects.filter(keywords=self).count()
        self.cached_usage_count = actual_count
        self.last_count_update = timezone.now()
        self.save(update_fields=['cached_usage_count', 'last_count_update'])

    @property
    def usage_count(self):
        """Возвращает кэшированное или вычисленное значение"""
        # Если кэш устарел (старше 1 дня) или отсутствует - обновляем
        if (not self.last_count_update or
                (timezone.now() - self.last_count_update).days > 1):
            self.update_cached_count()
        return self.cached_usage_count

    def get_fresh_usage_count(self):
        """Всегда получает свежее значение"""
        from .models import Game
        return Game.objects.filter(keywords=self).count()

    def __str__(self):
        category_name = self.category.name if self.category else "No Category"
        return f"{self.name} ({category_name})"

    class Meta:
        indexes = [
            models.Index(fields=['cached_usage_count']),
            models.Index(fields=['name']),
        ]

    @property
    def popularity_score(self):
        """Популярность равна количеству использований"""
        return float(self.usage_count)

    def save(self, *args, **kwargs):
        # При сохранении обновляем кэшированный счетчик
        if not self.pk:  # Если это новый объект
            super().save(*args, **kwargs)
            self.update_cached_count()
        else:
            super().save(*args, **kwargs)


class Genre(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class Platform(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)
    slug = models.SlugField(max_length=50, blank=True, null=True)

    def __str__(self):
        return self.name

    def get_platform_slug(self):
        """Возвращает slug для платформы для использования в URL иконок"""
        name_lower = self.name.lower()

        # PlayStation
        if 'playstation 5' in name_lower or 'ps5' in name_lower:
            return 'playstation5'
        elif 'playstation 4' in name_lower or 'ps4' in name_lower:
            return 'playstation4'
        elif 'playstation 3' in name_lower or 'ps3' in name_lower:
            return 'playstation3'
        elif 'playstation 2' in name_lower or 'ps2' in name_lower:
            return 'playstation2'
        elif 'playstation 1' in name_lower or 'ps1' in name_lower:
            return 'playstation'
        elif 'psp' in name_lower:
            return 'psp'
        elif 'vita' in name_lower:
            return 'vita'

        # Xbox
        elif 'xbox series x' in name_lower or 'xbox series s' in name_lower:
            return 'xbox-series-x'
        elif 'xbox one' in name_lower:
            return 'xbox-one'
        elif 'xbox 360' in name_lower:
            return 'xbox-360'
        elif 'xbox' in name_lower:
            return 'xbox'

        # Nintendo
        elif 'nintendo switch' in name_lower:
            return 'nintendo-switch'
        elif 'wii u' in name_lower:
            return 'wii-u'
        elif 'wii' in name_lower:
            return 'wii'
        elif 'nintendo 3ds' in name_lower:
            return 'nintendo-3ds'
        elif 'nintendo ds' in name_lower:
            return 'nintendo-ds'
        elif 'game boy' in name_lower:
            return 'game-boy'

        # PC
        elif 'windows' in name_lower:
            return 'windows'
        elif 'linux' in name_lower:
            return 'linux'
        elif 'mac' in name_lower or 'macos' in name_lower:
            return 'macos'

        # Mobile
        elif 'android' in name_lower:
            return 'android'
        elif 'ios' in name_lower:
            return 'ios'

        else:
            return 'default'

    def game_count(self):
        """Количество игр на этой платформе"""
        return self.game_set.count()

    @property
    def icon_class(self):
        """Возвращает CSS класс для иконки платформы"""
        slug = self.get_platform_slug()
        return f"platform-icon platform-{slug}"

    @property
    def icon_url(self):
        """Возвращает URL иконки платформы"""
        # Можно использовать локальные иконки или внешние
        slug = self.get_platform_slug()
        return f"/static/platforms/{slug}.png"
        # Или внешние иконки:
        # return f"https://images.igdb.com/igdb/image/upload/t_platform_logo/{self.igdb_id}.png"

    @property
    def display_name(self):
        """Возвращает отображаемое имя платформы"""
        name_lower = self.name.lower()

        # Исправляем только очень длинные названия
        if 'pc (microsoft windows)' in name_lower:
            return 'PC'
        elif 'xbox series x|s' in name_lower:
            return 'Xbox Series X/S'
        elif 'super nintendo entertainment system' in name_lower:
            return 'Super Nintendo'
        elif 'commodore c64/128/max' in name_lower:
            return 'Commodore 64'
        elif 'sega mega drive/genesis' in name_lower:
            return 'Sega Genesis'
        elif '3do interactive multiplayer' in name_lower:
            return '3DO'
        elif 'legacy mobile device' in name_lower:
            return 'Mobile'
        elif 'turbografx-16/pc engine cd' in name_lower:
            return 'TurboGrafx-CD'
        elif 'intellivision amico' in name_lower:
            return 'Intellivision'
        elif 'neo geo pocket color' in name_lower:
            return 'Neo Geo Pocket'
        elif 'wonderswan color' in name_lower:
            return 'WonderSwan'
        elif 'atari 8-bit' in name_lower:
            return 'Atari 8-bit'
        elif 'bbc microcomputer system' in name_lower:
            return 'BBC Micro'
        elif 'pc-9800 series' in name_lower:
            return 'PC-9800'
        elif 'pc-8800 series' in name_lower:
            return 'PC-8800'
        elif 'sharp mz-2200' in name_lower:
            return 'Sharp MZ'
        elif 'turbografx-16/pc engine' in name_lower:
            return 'TurboGrafx-16'
        elif 'nintendo entertainment system' in name_lower:
            return 'NES'
        elif 'meta quest' in name_lower:
            return 'Meta Quest'
        elif 'playstation vr2' in name_lower:
            return 'PS VR2'
        elif 'oculus quest' in name_lower:
            return 'Oculus Quest'
        elif 'blackberry os' in name_lower:
            return 'BlackBerry'
        elif 'sega cd' in name_lower:
            return 'Sega CD'
        elif 'oculus vr' in name_lower:
            return 'Oculus'
        elif 'windows' in name_lower:
            return 'PC'

        # Для остальных оставляем оригинальное название
        return self.name


class GameSimilarityCache(models.Model):
    """Кэш для предварительно рассчитанной схожести игр"""
    game1 = models.ForeignKey(Game, on_delete=models.CASCADE, related_name='similarity_as_source')
    game2 = models.ForeignKey(Game, on_delete=models.CASCADE, related_name='similarity_as_target')
    similarity_score = models.FloatField()
    calculated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['game1', 'game2']
        indexes = [
            models.Index(fields=['game1', '-similarity_score']),
            models.Index(fields=['game2', '-similarity_score']),
        ]
        verbose_name_plural = "Game similarity cache"

    def __str__(self):
        return f"{self.game1} -> {self.game2}: {self.similarity_score}%"


class Screenshot(models.Model):
    """Скриншоты игр из IGDB"""
    igdb_id = models.IntegerField(unique=True)
    game = models.ForeignKey(Game, on_delete=models.CASCADE, related_name='screenshots')
    image_url = models.URLField()
    width = models.IntegerField(default=1920)
    height = models.IntegerField(default=1080)

    # Можно добавить дополнительные поля
    is_primary = models.BooleanField(default=False)
    caption = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Screenshot {self.igdb_id} for {self.game.name}"

    class Meta:
        ordering = ['-is_primary', 'id']
