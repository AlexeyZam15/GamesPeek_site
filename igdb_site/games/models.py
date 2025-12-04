from django.db import models


class Country(models.Model):
    """Модель для стран"""
    igdb_id = models.IntegerField(unique=True, blank=True, null=True)
    name = models.CharField(max_length=100)
    code = models.CharField(max_length=10, blank=True, null=True)  # ISO код страны

    def __str__(self):
        return self.name

    class Meta:
        verbose_name_plural = "Countries"
        ordering = ['name']

class Company(models.Model):
    """Компании (разработчики и издатели)"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    country = models.ForeignKey(
        Country,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name='companies'
    )
    logo_url = models.URLField(blank=True, null=True)
    website = models.URLField(blank=True, null=True)

    # Типы компании
    DEVELOPER = 'developer'
    PUBLISHER = 'publisher'
    SUPPORTING = 'supporting'
    PORTING = 'porting'
    COMPANY_TYPE_CHOICES = [
        (DEVELOPER, 'Developer'),
        (PUBLISHER, 'Publisher'),
        (SUPPORTING, 'Supporting'),
        (PORTING, 'Porting'),
    ]

    def __str__(self):
        return self.name

    class Meta:
        verbose_name_plural = "Companies"
        ordering = ['name']


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
    usage_count = models.IntegerField(default=0)  # ← ДОБАВИТЬ ЭТО ПОЛЕ
    popularity_score = models.FloatField(default=0.0)  # ← И ЭТО

    def __str__(self):
        category_name = self.category.name if self.category else "No Category"
        return f"{self.name} ({category_name})"

    def update_popularity(self):
        """Обновляет счетчик использования и популярность"""
        self.usage_count = self.game_set.count()
        # Можно добавить более сложную формулу для популярности
        self.popularity_score = self.usage_count
        self.save()

    @property
    def popularity_level(self):
        """Возвращает уровень популярности (Low, Medium, High, Very High)"""
        if self.usage_count == 0:
            return "Unused"
        elif self.usage_count <= 5:
            return "Low"
        elif self.usage_count <= 20:
            return "Medium"
        elif self.usage_count <= 100:
            return "High"
        else:
            return "Very High"


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


class Game(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    summary = models.TextField(blank=True, null=True)
    storyline = models.TextField(blank=True, null=True)
    rating = models.FloatField(blank=True, null=True)
    rating_count = models.IntegerField(default=0)
    first_release_date = models.DateTimeField(blank=True, null=True)

    # Существующие связи
    genres = models.ManyToManyField(Genre, blank=True)
    platforms = models.ManyToManyField(Platform, blank=True)
    keywords = models.ManyToManyField(Keyword, blank=True)

    # Связи с сериями
    series = models.ManyToManyField(
        Series,
        blank=True,
        related_name='games'
    )

    # Для порядка игр в серии (если применимо)
    series_order = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Порядковый номер в серии (1, 2, 3...). Если игра в нескольких сериях, это относится к основной серии."
    )

    # Связи с компаниями
    developers = models.ManyToManyField(
        Company,
        related_name='developed_games',
        blank=True
    )
    publishers = models.ManyToManyField(
        Company,
        related_name='published_games',
        blank=True
    )

    # Новые категории
    themes = models.ManyToManyField(Theme, blank=True)
    player_perspectives = models.ManyToManyField(PlayerPerspective, blank=True)
    game_modes = models.ManyToManyField(GameMode, blank=True)

    cover_url = models.URLField(blank=True, null=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['-rating_count']

    # ИСПРАВЛЕННЫЕ PROPERTY ДЛЯ СЕРИЙ
    @property
    def is_part_of_series(self):
        """Принадлежит ли игра к какой-либо серии"""
        return self.series.exists()

    @property
    def main_series(self):
        """Возвращает первую (основную) серию игры"""
        return self.series.first()

    @property
    def display_series_info(self):
        """Отображаемая информация о сериях"""
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
        """Возвращает все игры из той же серии/серий"""
        if series:
            # Если указана конкретная серия
            return series.games.exclude(id=self.id).order_by('first_release_date')
        elif self.series.exists():
            # Если не указана серия, берем первую (основную)
            main_series = self.series.first()
            return main_series.games.exclude(id=self.id).order_by('first_release_date')
        return Game.objects.none()

    def get_all_series_games(self):
        """Возвращает все игры из всех серий этой игры"""
        all_games = Game.objects.none()
        for series in self.series.all():
            series_games = series.games.exclude(id=self.id)
            all_games = all_games.union(series_games)
        return all_games.distinct()

    @property
    def series_count(self):
        """Количество серий, к которым принадлежит игра"""
        return self.series.count()

    # PROPERTY ДЛЯ КОМПАНИЙ
    @property
    def main_developer(self):
        """Основной разработчик (первый в списке)"""
        return self.developers.first()

    @property
    def main_publisher(self):
        """Основной издатель (первый в списке)"""
        return self.publishers.first()

    @property
    def developer_names(self):
        """Список имен разработчиков"""
        return list(self.developers.values_list('name', flat=True))

    @property
    def publisher_names(self):
        """Список имен издателей"""
        return list(self.publishers.values_list('name', flat=True))

    # PROPERTY ДЛЯ ТЕМ И ПЕРСПЕКТИВ
    @property
    def theme_names(self):
        """Список тем"""
        return list(self.themes.values_list('name', flat=True))

    @property
    def perspective_names(self):
        """Список перспектив"""
        return list(self.player_perspectives.values_list('name', flat=True))

    @property
    def game_mode_names(self):
        """Список режимов игры"""
        return list(self.game_modes.values_list('name', flat=True))

    # Существующие property для ключевых слов
    @property
    def gameplay_keywords(self):
        return self.keywords.filter(category__name='Gameplay')

    @property
    def setting_keywords(self):
        """Возвращает только ключевые слова сеттинга"""
        return self.keywords.filter(category__name='Setting')

    @property
    def genre_keywords(self):
        """Возвращает только ключевые слова жанров"""
        return self.keywords.filter(category__name='Genre')

    @property
    def narrative_keywords(self):
        """Возвращает только нарративные ключевые слова"""
        return self.keywords.filter(category__name='Narrative')

    @property
    def character_keywords(self):
        """Возвращает только ключевые слова персонажей"""
        return self.keywords.filter(category__name='Characters')

    @property
    def technical_keywords(self):
        """Возвращает только технические ключевые слова"""
        return self.keywords.filter(category__name='Technical')

    @property
    def graphics_keywords(self):
        """Возвращает только ключевые слова графики"""
        return self.keywords.filter(category__name='Graphics')

    @property
    def platform_keywords(self):
        """Возвращает только ключевые слова платформ"""
        return self.keywords.filter(category__name='Platform')

    @property
    def multiplayer_keywords(self):
        """Возвращает только мультиплеерные ключевые слова"""
        return self.keywords.filter(category__name='Multiplayer')

    @property
    def achievement_keywords(self):
        """Возвращает только ключевые слова достижений"""
        return self.keywords.filter(category__name='Achievements')

    @property
    def audio_keywords(self):
        """Возвращает только аудио ключевые слова"""
        return self.keywords.filter(category__name='Audio')

    @property
    def context_keywords(self):
        """Возвращает только контекстные ключевые слова"""
        return self.keywords.filter(category__name='Context')

    @property
    def development_keywords(self):
        """Возвращает только ключевые слова разработки"""
        return self.keywords.filter(category__name='Development')

    def get_keywords_by_category(self, category_name):
        """Универсальный метод для получения ключевых слов по категории"""
        return self.keywords.filter(category__name=category_name)


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
