"""
Команда Django для генерации описаний геймплея для игр без rawg_description
с использованием GigaChat API (бесплатный тариф, работает в России).
"""

import time
import logging
import requests
import urllib3
import uuid
from typing import Optional, Dict, Any
from django.core.management.base import BaseCommand
from django.db import models
from django.conf import settings

from games.models import Game

# Отключаем SSL предупреждения для разработки
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Команда Django для генерации описаний геймплея через GigaChat API.
    """

    help = 'Генерирует английские описания геймплея для игр без rawg_description через GigaChat'

    def add_arguments(self, parser):
        """Добавляет аргументы командной строки."""
        parser.add_argument(
            '--name',
            type=str,
            help='Точное название игры для обработки. Если не найдено, ищет самую популярную по имени.',
            default=None
        )
        parser.add_argument(
            '--output-file',
            type=str,
            help='Путь к файлу для сохранения описаний.',
            default='descriptions_output.txt'
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Ограничить количество обрабатываемых игр (для тестирования).',
            default=None
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только показать какие игры будут обработаны, без генерации.',
            default=False
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительно перезаписать существующие описания (по умолчанию пропускает игры с описанием).',
            default=False
        )

    def __init__(self):
        super().__init__()
        self.auth_key = None
        self.access_token = None
        self.token_expires_at = 0
        self.games_processed = 0
        self.games_failed = 0
        self.start_time = None
        self.auth_url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        self.api_url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

    def build_game_context(self, game: Game) -> str:
        """
        Собирает информацию об игре и добавляет полный файл с описаниями жанров.
        """
        context_parts = []

        # Название игры
        context_parts.append(f"[GAME NAME] {game.name}")

        # Данные из базы
        if game.summary:
            context_parts.append(f"[SUMMARY] {game.summary}")
        if game.storyline:
            context_parts.append(f"[STORYLINE] {game.storyline}")
        if game.wiki_description:
            context_parts.append(f"[WIKI DESCRIPTION] {game.wiki_description}")

        # Полный файл с описаниями всех жанров и тем
        genre_descriptions = self.load_genre_descriptions()
        if genre_descriptions:
            context_parts.append(f"\n[GENRE AND THEME DESCRIPTIONS]\n{genre_descriptions}")

        return '\n'.join(context_parts)

    def load_genre_descriptions(self) -> str:
        """
        Загружает полный файл с описаниями жанров и тем.
        """
        file_path = 'descriptions.txt'

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return content
        except FileNotFoundError:
            self.stdout.write(self.style.WARNING(f'⚠ Файл {file_path} не найден'))
            return ""
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка загрузки: {str(e)}'))
            return ""

    def setup_api(self) -> bool:
        """
        Настраивает GigaChat API.

        Returns:
            True если настройка успешна, False в противном случае
        """
        # Получаем Authorization key из настроек
        self.auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', None)

        if not self.auth_key:
            self.stdout.write(self.style.ERROR(
                'GIGACHAT_AUTH_KEY должен быть указан в settings.py\n\n'
                'Добавьте в settings.py:\n'
                "GIGACHAT_AUTH_KEY = 'MDE5ZDE1NWQtZDQ5Ny03MjVlLWIxNDQtNDdhMmRhY2ZjNDBmOjAyNTI3N2Q4LTM4MzQtNDJjYy04OWY4LTRlZjZjOGY4NDFlOA=='"
            ))
            return False

        self.stdout.write(self.style.SUCCESS(f'✅ GigaChat настроен\n'))

        # Получаем токен доступа
        return self.get_access_token()

    def handle(self, *args, **options):
        """Основной метод выполнения команды."""
        self.start_time = time.time()

        # Setup GigaChat
        if not self.setup_api():
            return

        # Set output file path
        output_file_path = options.get('output_file')

        # Get games to process
        games = self.get_games_to_process(options)

        if not games.exists():
            self.stdout.write(self.style.WARNING('Нет игр для обработки.'))
            return

        # Display info
        total = games.count()
        self.stdout.write(f'📊 Игр к обработке: {total}')
        self.stdout.write(f'📁 Выходной файл: {output_file_path}\n')

        if options.get('dry_run'):
            self.display_dry_run(games)
            return

        # Process games
        self.process_games(games, options)

        # Show summary (только один раз)
        self.show_summary()

    def get_access_token(self) -> bool:
        """
        Получает токен доступа GigaChat через Authorization key.

        Returns:
            True если токен получен, False в противном случае
        """
        try:
            # Генерируем уникальный RqUID
            rquid = str(uuid.uuid4())

            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
                'RqUID': rquid,
                'Authorization': f'Basic {self.auth_key}',
            }

            data = {
                'scope': 'GIGACHAT_API_PERS',
            }

            self.stdout.write('   Получение токена GigaChat...')
            self.stdout.write(f'   RqUID: {rquid}')

            response = requests.post(
                self.auth_url,
                headers=headers,
                data=data,
                timeout=30,
                verify=False
            )

            self.stdout.write(f'   Статус ответа: {response.status_code}')

            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get('access_token')
                expires_in = result.get('expires_in', 3600)
                self.token_expires_at = time.time() + expires_in - 60

                self.stdout.write(self.style.SUCCESS(f'   ✅ Токен получен, истекает через {expires_in} секунд'))
                return True
            else:
                self.stdout.write(self.style.ERROR(f'   ❌ Ошибка получения токена: {response.status_code}'))
                self.stdout.write(f'   Ответ: {response.text}')
                return False

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка при получении токена: {str(e)}'))
            return False

    def ensure_valid_token(self) -> bool:
        """
        Проверяет валидность токена, обновляет при необходимости.

        Returns:
            True если токен валиден, False в противном случае
        """
        if not self.access_token or time.time() >= self.token_expires_at:
            self.stdout.write('   Токен истек, обновление...')
            return self.get_access_token()
        return True

    def get_games_to_process(self, options: Dict[str, Any]) -> models.QuerySet:
        """
        Получает QuerySet игр для обработки на основе опций команды.

        Args:
            options: Словарь с опциями команды

        Returns:
            QuerySet объектов Game
        """
        game_name = options.get('name')
        force = options.get('force', False)

        if game_name:
            return self.get_specific_game(game_name)

        # Если force=True, берем все игры, иначе только без описания
        if force:
            queryset = Game.objects.all()
        else:
            queryset = Game.objects.filter(
                models.Q(rawg_description__isnull=True) | models.Q(rawg_description='')
            )

        if options.get('limit'):
            queryset = queryset[:options['limit']]

        return queryset

    def get_specific_game(self, game_name: str) -> models.QuerySet:
        """
        Находит игру по точному имени, с fallback на самую популярную по части имени.

        Args:
            game_name: Название игры для поиска

        Returns:
            QuerySet с одной игрой или пустой QuerySet
        """
        # Сначала точное совпадение
        exact_match = Game.objects.filter(name__iexact=game_name)

        if exact_match.exists():
            self.stdout.write(self.style.SUCCESS(f'✓ Найдено точное совпадение: {exact_match.first().name}'))
            return exact_match

        # Если нет точного совпадения, ищем по имени и берем самую популярную
        name_match = Game.objects.filter(name__icontains=game_name).order_by('-rating_count')

        if name_match.exists():
            best_match = name_match.first()
            self.stdout.write(
                self.style.WARNING(
                    f'⚠ Нет точного совпадения для "{game_name}". '
                    f'Использую самую популярную: "{best_match.name}"'
                )
            )
            return Game.objects.filter(id=best_match.id)

        self.stdout.write(self.style.ERROR(f'✗ Игры не найдены по запросу "{game_name}"'))
        return Game.objects.none()

    def display_dry_run(self, games: models.QuerySet) -> None:
        """
        Отображает список игр для обработки в режиме dry-run.

        Args:
            games: QuerySet игр для обработки
        """
        self.stdout.write(self.style.WARNING('🔍 РЕЖИМ DRY RUN - Описания генерироваться не будут\n'))

        for idx, game in enumerate(games, 1):
            self.stdout.write(f'{idx:3}. {game.name}')
            if game.summary:
                summary_preview = game.summary[:80] + '...' if len(game.summary) > 80 else game.summary
                self.stdout.write(f'     Сводка: {summary_preview}')

        self.stdout.write(f'\n✅ Будет обработано {games.count()} игр.')

    def process_games(self, games: models.QuerySet, options: Dict[str, Any]) -> None:
        """
        Обрабатывает все игры и генерирует описания.

        Args:
            games: QuerySet игр для обработки
            options: Опции команды
        """
        output_file_path = options.get('output_file')
        force = options.get('force', False)

        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            output_file.write('=' * 80 + '\n')
            output_file.write('GAMEPLAY DESCRIPTIONS GENERATED BY GIGACHAT\n')
            output_file.write(f'Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
            output_file.write('=' * 80 + '\n\n')

            for idx, game in enumerate(games, 1):
                self.stdout.write(f'\n[{idx}/{games.count()}] Обработка: {game.name}')

                # Пропускаем если уже есть описание и не force
                if not force and game.rawg_description:
                    self.stdout.write(
                        self.style.WARNING(f'  ⏭ Пропуск (уже есть описание, используйте --force для перезаписи)'))
                    output_file.write(f'GAME: {game.name}\n')
                    output_file.write(f'ID: {game.id}\n')
                    output_file.write('-' * 80 + '\n')
                    output_file.write(f'[SKIPPED] Already has description\n')
                    output_file.write('=' * 80 + '\n\n')
                    continue

                # Проверяем валидность токена
                if not self.ensure_valid_token():
                    self.stdout.write(self.style.ERROR(f'  ✗ Не удалось обновить токен'))
                    self.games_failed += 1
                    continue

                try:
                    description = self.generate_description(game, output_file)

                    if description:
                        # Сохраняем в базу
                        game.rawg_description = description
                        game.save(update_fields=['rawg_description'])

                        # Сохраняем в файл
                        output_file.write(f'GAME: {game.name}\n')
                        output_file.write(f'ID: {game.id}\n')
                        output_file.write(f'IGDB ID: {game.igdb_id}\n')
                        output_file.write('-' * 80 + '\n')
                        output_file.write(f'{description}\n')
                        output_file.write('=' * 80 + '\n\n')
                        output_file.flush()

                        self.games_processed += 1
                        self.stdout.write(self.style.SUCCESS(f'  ✓ Сохранено'))
                    else:
                        self.games_failed += 1
                        self.stdout.write(self.style.ERROR(f'  ✗ Не удалось сгенерировать'))

                        output_file.write(f'GAME: {game.name}\n')
                        output_file.write(f'ID: {game.id}\n')
                        output_file.write('-' * 80 + '\n')
                        output_file.write(f'[FAILED] Could not generate description\n')
                        output_file.write('=' * 80 + '\n\n')
                        output_file.flush()

                except Exception as e:
                    self.games_failed += 1
                    self.stdout.write(self.style.ERROR(f'  ✗ Ошибка: {str(e)}'))

                time.sleep(1)

    def generate_description(self, game: Game, log_file=None) -> Optional[str]:
        """
        Генерирует описание геймплея через GigaChat.
        """
        try:
            context = self.build_game_context(game)
            prompt = self.build_prompt(game.name, context)

            # Логируем полный промпт в файл
            if log_file:
                log_file.write(f"\n{'=' * 80}\n")
                log_file.write(f"GAME: {game.name}\n")
                log_file.write(f"{'=' * 80}\n")
                log_file.write(f"FULL PROMPT:\n{'-' * 80}\n{prompt}\n{'-' * 80}\n")

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }

            request_body = {
                'model': 'GigaChat',
                'messages': [
                    {
                        'role': 'system',
                        'content': 'You are a video game expert. You analyze game information and select appropriate genres and themes from the provided lists. You write accurate gameplay descriptions.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'temperature': 0.6,
                'max_tokens': 1500,
            }

            response = requests.post(
                self.api_url,
                headers=headers,
                json=request_body,
                timeout=60,
                verify=False
            )

            if response.status_code == 200:
                result = response.json()

                if 'choices' in result and len(result['choices']) > 0:
                    description = result['choices'][0]['message']['content']
                    description = self.clean_description(description)

                    return description
                else:
                    return None
            else:
                if response.status_code == 401:
                    self.access_token = None
                return None

        except Exception as e:
            if log_file:
                log_file.write(f"ERROR: {str(e)}\n")
            return None

    def build_prompt(self, game_name: str, context: str) -> str:
        """
        Создает промпт для GigaChat с полным файлом описаний.
        """
        if context:
            return f"""Write a gameplay description for the video game "{game_name}".

    Here is information about this game and all genre/theme descriptions:
    {context}

    INSTRUCTIONS:

    STEP 1 - ANALYZE THE GAME:
    Carefully read the [SUMMARY], [STORYLINE], [WIKI DESCRIPTION], and [GAME NAME].

    STEP 2 - DETERMINE GENRES:
    Go through each genre in [GENRE AND THEME DESCRIPTIONS] and check if it matches the game. Consider:
    - Core gameplay mechanics
    - How the player interacts with the game
    - The structure and systems
    Select ALL genres that clearly fit.

    STEP 3 - DETERMINE THEMES:
    Go through each theme in [GENRE AND THEME DESCRIPTIONS] and check if it matches the game. Consider:
    - Setting and world
    - Story elements
    - Atmosphere and tone
    Select ALL themes that clearly fit.

    STEP 4 - WRITE DESCRIPTION:
    Start with:
    Genres: [comma separated list of selected genres]
    Themes: [comma separated list of selected themes]

    Then write 3-4 paragraphs about gameplay mechanics.

    RULES:
    - Be thorough - list ALL matching genres and themes
    - Base decisions on the game information provided
    - Use your knowledge of this specific game
    - No markdown, no symbols like ###, ---, **
    - Plain text only

    Write the gameplay description:"""
        else:
            return f"""Write a gameplay description for the video game "{game_name}".

    Start with:
    Genres: [list of genres]
    Themes: [list of themes]

    Use plain text only, no markdown or symbols.

    Gameplay description:"""

    def clean_description(self, description: str) -> str:
        """
        Очищает сгенерированное описание.

        Args:
            description: Сырое описание

        Returns:
            Очищенное описание
        """
        import re

        # Удаляем служебные заголовки
        description = re.sub(r'^Gameplay description:?\s*', '', description, flags=re.IGNORECASE)
        description = re.sub(r'^DESCRIPTION:?\s*', '', description, flags=re.IGNORECASE)

        # Убираем лишние пустые строки
        description = re.sub(r'\n\s*\n\s*\n', '\n\n', description)

        return description.strip()

    def show_summary(self) -> None:
        """Отображает статистику выполнения."""
        elapsed_time = time.time() - self.start_time

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'✅ Успешно сгенерировано: {self.games_processed}')
        self.stdout.write(f'❌ Не удалось: {self.games_failed}')
        self.stdout.write(f'⏱️  Затрачено времени: {elapsed_time:.2f} секунд')
        self.stdout.write('=' * 60)
