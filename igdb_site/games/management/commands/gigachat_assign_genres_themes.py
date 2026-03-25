"""
Команда Django для автоматического определения и присвоения жанров и тем играм
на основе анализа через GigaChat API.
"""

import time
import logging
import json
import os
import traceback
import signal
import sys
from typing import Optional, Dict, Any, List, Tuple
from django.core.management.base import BaseCommand
from django.db import models
from django.conf import settings

from games.models import Game, Genre, Theme

logger = logging.getLogger(__name__)


class ProgressBar:
    """
    Простой прогресс-бар с расчетом времени.
    """

    def __init__(self, total, width=50, stdout=None):
        self.total = total
        self.width = width
        self.current = 0
        self.start_time = time.time()
        self.stdout = stdout or sys.stdout
        self.last_update = 0

    def update(self, current=None, suffix=""):
        """Обновляет прогресс-бар."""
        if current is not None:
            self.current = current

        # Не позволяем current превышать total
        if self.current > self.total:
            self.current = self.total

        # Расчет времени
        elapsed = time.time() - self.start_time
        if self.current > 0:
            rate = self.current / elapsed
            eta = (self.total - self.current) / rate if rate > 0 else 0
        else:
            eta = 0

        # Форматирование времени
        elapsed_str = self._format_time(elapsed)
        eta_str = self._format_time(eta) if eta > 0 else "?"

        # Расчет процентов и заполнения
        if self.total > 0:
            percent = self.current / self.total * 100
            filled = int(self.width * self.current / self.total)
        else:
            percent = 0
            filled = 0

        bar = '█' * filled + '░' * (self.width - filled)

        # Формирование строки прогресс-бара
        progress_line = f'\r[{bar}] {percent:.1f}% | {self.current}/{self.total} | ⏱️ {elapsed_str} | ⏳ ETA: {eta_str}'
        if suffix:
            progress_line += f' | {suffix}'

        # Запись в stdout без перевода строки
        self.stdout.write(progress_line, ending='')
        self.stdout.flush()
        self.last_update = time.time()

    def finish(self, suffix=""):
        """Завершает прогресс-бар."""
        self.update(self.total, suffix)
        self.stdout.write('\n')

    def _format_time(self, seconds):
        """Форматирует время в ЧЧ:ММ:СС."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.0f}m {seconds % 60:.0f}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"


class Command(BaseCommand):
    """
    Команда Django для автоматического определения жанров и тем через GigaChat API
    и присвоения их играм.
    """

    help = 'Автоматически определяет жанры и темы для игр через GigaChat и присваивает их'

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
        self.genres_data = []
        self.themes_data = []

        # Кэши для существующих жанров и тем в БД
        self.genre_cache = {}
        self.theme_cache = {}

        # Флаги для обработки прерывания
        self.interrupted = False
        self.current_game = None
        self.current_game_index = 0
        self.total_games = 0

        # Прогресс-бар
        self.progress_bar = None

        # Настройка обработчиков сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """
        Обработчик сигналов прерывания (Ctrl+C, SIGTERM).
        """
        # Если уже был сигнал, не выводим повторно
        if self.interrupted:
            return

        # Устанавливаем флаг прерывания
        self.interrupted = True

        # Очищаем текущую строку прогресс-бара
        self.stdout.write('\n', ending='')
        self.stdout.flush()

        # Выводим сообщение
        self.stdout.write(self.style.WARNING('⚠ Получен сигнал прерывания...'))
        self.stdout.write(self.style.WARNING('⚠ Дожидаемся завершения текущей игры...'))
        self.stdout.flush()

    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            help='Точное название игры для обработки. Если не найдено, ищет самую популярную по имени.',
            default=None
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
            help='Только показать обнаруженные жанры и темы для каждой игры, не сохранять в БД.',
            default=False
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительно переопределить существующие жанры и темы (по умолчанию пропускает игры с уже присвоенными).',
            default=False
        )
        parser.add_argument(
            '--output-file',
            type=str,
            help='Путь к файлу для сохранения отчета о найденных жанрах и темах.',
            default='genres_themes_analysis.txt'
        )
        parser.add_argument(
            '--error-file',
            type=str,
            help='Путь к файлу для сохранения ошибок с полной информацией.',
            default='genres_themes_errors.txt'
        )
        parser.add_argument(
            '--summary-file',
            type=str,
            help='Путь к краткому файлу с результатами (только игра и найденные жанры/темы).',
            default='genres_themes_summary.txt'
        )
        parser.add_argument(
            '--resume',
            action='store_true',
            help='Возобновить выполнение с последней обработанной игры.',
            default=False
        )
        parser.add_argument(
            '--resume-file',
            type=str,
            help='Файл для сохранения состояния возобновления.',
            default='genres_themes_resume.txt'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Показывать подробную информацию по каждой игре.',
            default=False
        )

    def log_message(self, message, style='INFO', verbose_only=False):
        """
        Выводит сообщение, учитывая режим verbose.
        Если verbose_only=True, сообщение выводится только при --verbose.
        """
        if verbose_only and not self.verbose:
            return

        # Если есть активный прогресс-бар, переводим строку перед выводом
        if self.progress_bar and self.progress_bar.current > 0:
            self.stdout.write('\n', ending='')

        if style == 'SUCCESS':
            self.stdout.write(self.style.SUCCESS(message))
        elif style == 'ERROR':
            self.stdout.write(self.style.ERROR(message))
        elif style == 'WARNING':
            self.stdout.write(self.style.WARNING(message))
        else:
            self.stdout.write(message)

        # После вывода сообщения обновляем прогресс-бар
        if self.progress_bar and self.progress_bar.current > 0:
            self.progress_bar.update()

    def handle(self, *args, **options):
        self.start_time = time.time()
        self.verbose = options.get('verbose', False)

        if not self.setup_api():
            return

        self.load_genre_theme_descriptions()
        self.load_existing_genres_themes()

        output_file_path = options.get('output_file')
        error_file_path = options.get('error_file')
        summary_file_path = options.get('summary_file')
        resume_file_path = options.get('resume_file')
        resume = options.get('resume', False)
        dry_run = options.get('dry_run', False)

        games = self.get_games_to_process(options)

        # Создаем список для возобновления
        processed_games = set()
        if resume:
            processed_games = self.load_resume_state(resume_file_path)
            self.log_message(f'📋 Режим возобновления: пропускаем {len(processed_games)} уже обработанных игр')

        if not games.exists():
            self.log_message('Нет игр для обработки.', 'WARNING')
            return

        total = games.count()
        self.total_games = total

        # Инициализация прогресс-бара
        self.progress_bar = ProgressBar(total, width=40, stdout=self.stdout)

        self.log_message(f'📊 Игр к обработке: {total}')
        self.log_message(f'📁 Выходной файл: {output_file_path}')
        self.log_message(f'📁 Файл ошибок: {error_file_path}')
        self.log_message(f'📁 Краткий файл: {summary_file_path}')
        self.log_message(f'📁 Файл состояния: {resume_file_path}')

        if dry_run:
            self.log_message('🔍 РЕЖИМ DRY RUN - изменения в БД НЕ будут применены\n', 'WARNING')

        if resume and processed_games:
            self.log_message(f'📋 Продолжаем с {len(processed_games) + 1} игры\n')

        self.process_games(games, options, output_file_path, error_file_path,
                           summary_file_path, resume_file_path, processed_games, dry_run)

        # Завершаем прогресс-бар ТОЛЬКО если не было прерывания
        if not self.interrupted and self.progress_bar:
            # Получаем финальный суффикс
            suffix = f"✅ {self.games_processed} успешно | ❌ {self.games_failed} ошибок"
            self.progress_bar.finish(suffix)

        self.show_summary()

    def load_resume_state(self, resume_file_path: str) -> set:
        """
        Загружает состояние из файла для возобновления.
        """
        processed = set()
        try:
            if os.path.exists(resume_file_path):
                with open(resume_file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            try:
                                game_id = int(line)
                                processed.add(game_id)
                            except ValueError:
                                continue
        except Exception as e:
            self.log_message(f'⚠ Не удалось загрузить состояние: {e}', 'WARNING', verbose_only=True)
        return processed

    def save_resume_state(self, resume_file_path: str, game_id: int):
        """
        Сохраняет состояние в файл для возобновления.
        """
        try:
            with open(resume_file_path, 'a', encoding='utf-8') as f:
                f.write(f'{game_id}\n')
                f.flush()
        except Exception as e:
            self.log_message(f'⚠ Не удалось сохранить состояние: {e}', 'WARNING', verbose_only=True)

    def setup_api(self) -> bool:
        """Настройка API GigaChat"""
        self.auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', None)

        if not self.auth_key:
            self.log_message('GIGACHAT_AUTH_KEY должен быть указан в settings.py', 'ERROR')
            return False

        self.log_message('✅ GigaChat настроен\n', 'SUCCESS')
        return self.get_access_token()

    def get_access_token(self) -> bool:
        """Получение токена доступа к GigaChat API"""
        import requests
        import uuid
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        try:
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

            self.log_message('   Получение токена GigaChat...', verbose_only=True)
            response = requests.post(
                self.auth_url,
                headers=headers,
                data=data,
                timeout=30,
                verify=False
            )

            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get('access_token')
                expires_in = result.get('expires_in', 3600)
                self.token_expires_at = time.time() + expires_in - 60

                self.log_message('   ✅ Токен получен', 'SUCCESS', verbose_only=True)
                return True
            else:
                self.log_message(f'   ❌ Ошибка получения токена: {response.status_code}', 'ERROR')
                return False

        except Exception as e:
            self.log_message(f'   ❌ Ошибка: {str(e)}', 'ERROR')
            return False

    def ensure_valid_token(self) -> bool:
        """Проверка и обновление токена при необходимости"""
        if not self.access_token or time.time() >= self.token_expires_at:
            self.log_message('   Токен истек, обновление...', verbose_only=True)
            return self.get_access_token()
        return True

    def load_genre_theme_descriptions(self):
        """Загрузка описаний жанров и тем из файла"""
        file_path = 'descriptions.txt'

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            data = json.loads(content)

            if isinstance(data, dict):
                self.genres_data = data.get('genres', [])
                self.themes_data = data.get('themes', [])
                self.log_message(f'✅ Загружено жанров: {len(self.genres_data)}, тем: {len(self.themes_data)}',
                                 'SUCCESS')
            else:
                self.log_message('❌ Файл должен содержать объект с полями genres и themes', 'ERROR')

        except FileNotFoundError:
            self.log_message(f'❌ Файл {file_path} не найден', 'ERROR')
            self.genres_data = []
            self.themes_data = []
        except json.JSONDecodeError as e:
            self.log_message(f'❌ Ошибка парсинга JSON: {e}', 'ERROR')
            self.genres_data = []
            self.themes_data = []
        except Exception as e:
            self.log_message(f'❌ Ошибка: {str(e)}', 'ERROR')
            self.genres_data = []
            self.themes_data = []

    def load_existing_genres_themes(self):
        """Загрузка существующих жанров и тем из БД в кэш"""
        for genre in Genre.objects.all():
            self.genre_cache[genre.name.lower()] = genre

        for theme in Theme.objects.all():
            self.theme_cache[theme.name.lower()] = theme

        self.log_message(f'✅ Загружено из БД: {len(self.genre_cache)} жанров, {len(self.theme_cache)} тем', 'SUCCESS')

    def get_games_to_process(self, options: Dict[str, Any]) -> models.QuerySet:
        """Получение списка игр для обработки"""
        game_name = options.get('name')
        force = options.get('force', False)

        if game_name:
            return self.get_specific_game(game_name)

        if force:
            queryset = Game.objects.all()
        else:
            # Получаем игры без rawg_description
            queryset = Game.objects.filter(
                models.Q(rawg_description__isnull=True) | models.Q(rawg_description='')
            )

        if options.get('limit'):
            queryset = queryset[:options['limit']]

        return queryset

    def get_specific_game(self, game_name: str) -> models.QuerySet:
        """Поиск конкретной игры по названию"""
        exact_match = Game.objects.filter(name__iexact=game_name)

        if exact_match.exists():
            self.log_message(f'✓ Найдено точное совпадение: {exact_match.first().name}', 'SUCCESS')
            return exact_match

        name_match = Game.objects.filter(name__icontains=game_name).order_by('-rating_count')

        if name_match.exists():
            best_match = name_match.first()
            self.log_message(
                f'⚠ Нет точного совпадения для "{game_name}". Использую: "{best_match.name}"',
                'WARNING'
            )
            return Game.objects.filter(id=best_match.id)

        self.log_message(f'✗ Игры не найдены', 'ERROR')
        return Game.objects.none()

    def build_compact_game_context(self, game: Game) -> str:
        """Сбор контекстной информации об игре для анализа"""
        context_parts = []

        name_text = f"Name: {game.name}"
        context_parts.append(name_text)

        if game.summary:
            summary_text = f"Summary: {game.summary}"
            context_parts.append(summary_text)

        if game.storyline:
            storyline_text = f"Story: {game.storyline}"
            context_parts.append(storyline_text)

        if game.wiki_description:
            wiki_text = f"Wiki: {game.wiki_description}"
            context_parts.append(wiki_text)

        return '\n'.join(context_parts)

    def build_genres_analysis_prompt(self, game_name: str, context: str) -> str:
        """
        Создает промпт с требованием вывода в JSON формате для анализа жанров и тем.
        """
        # Формируем списки жанров и тем для явного перечисления
        genre_names = [g.get('name', '') for g in self.genres_data if g.get('name')]
        theme_names = [t.get('name', '') for t in self.themes_data if t.get('name')]

        return f"""Analyze the game "{game_name}" and determine which genres and themes from the provided lists apply.

Game Information:
{context}

AVAILABLE GENRES (you MUST ONLY choose from this exact list, no others):
{', '.join(genre_names)}

AVAILABLE THEMES (you MUST ONLY choose from this exact list, no others):
{', '.join(theme_names)}

For reference, here are the definitions:
{json.dumps({"genres": self.genres_data, "themes": self.themes_data}, ensure_ascii=False, indent=2)}

CRITICAL RULES:
1. You MUST ONLY use genres from the AVAILABLE GENRES list above
2. You MUST ONLY use themes from the AVAILABLE THEMES list above
3. DO NOT invent new genres or themes like "Other Genres" or "Other Themes"
4. EVERY genre and theme from the available lists MUST appear in your output
5. Output ONLY valid JSON, no other text

Output format:
{{
  "genres": [
    {{"name": "Action", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}},
    {{"name": "Adventure", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}},
    ... (continue for ALL available genres)
  ],
  "themes": [
    {{"name": "Drama", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}},
    {{"name": "Fantasy", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}},
    ... (continue for ALL available themes)
  ]
}}"""

    def analyze_game_genres_themes(self, game: Game) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
        """
        Анализирует жанры и темы игры через GigaChat.
        Возвращает tuple (анализ_в_виде_словаря, промпт, raw_response) или (None, None, None) при ошибке.
        """
        import requests
        import json
        import re
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        try:
            context = self.build_compact_game_context(game)
            prompt = self.build_genres_analysis_prompt(game.name, context)

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }

            request_body = {
                'model': 'GigaChat',
                'messages': [
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'temperature': 0.3,
                'max_tokens': 8000,
            }

            response = requests.post(
                self.api_url,
                headers=headers,
                json=request_body,
                timeout=120,
                verify=False
            )

            raw_response = response.text

            if response.status_code == 200:
                result = response.json()

                if 'choices' in result and len(result['choices']) > 0:
                    analysis_text = result['choices'][0]['message']['content']

                    # Парсим JSON ответ с восстановлением некорректного JSON
                    try:
                        # Извлекаем JSON из ответа
                        json_start = analysis_text.find('{')
                        json_end = analysis_text.rfind('}') + 1
                        if json_start != -1 and json_end > json_start:
                            json_text = analysis_text[json_start:json_end]
                        else:
                            json_text = analysis_text

                        # Пробуем распарсить напрямую
                        try:
                            data = json.loads(json_text)
                        except json.JSONDecodeError as e:
                            # Восстанавливаем JSON
                            lines = json_text.split('\n')
                            bracket_count = 0

                            for line in lines:
                                bracket_count += line.count('{')
                                bracket_count -= line.count('}')

                            fixed_json = json_text

                            if bracket_count > 0:
                                fixed_json += '}' * bracket_count

                            fixed_json = re.sub(r',\s*}', '}', fixed_json)
                            fixed_json = re.sub(r',\s*]', ']', fixed_json)

                            try:
                                data = json.loads(fixed_json)
                            except json.JSONDecodeError:
                                data = self.extract_genres_themes_with_regex(json_text)

                                if not data:
                                    return None, prompt, raw_response

                        return data, prompt, raw_response

                    except Exception as e:
                        self.log_message(f'  ✗ Ошибка парсинга JSON: {e}', 'ERROR', verbose_only=True)
                        return None, prompt, raw_response
                else:
                    return None, prompt, raw_response
            else:
                if response.status_code == 401:
                    self.access_token = None
                return None, None, raw_response

        except Exception as e:
            self.log_message(f'  ✗ Ошибка при анализе: {str(e)}', 'ERROR', verbose_only=True)
            return None, None, None

    def extract_genres_themes_with_regex(self, json_text: str) -> Optional[Dict]:
        """
        Извлекает жанры и темы из некорректного JSON через регулярные выражения.
        """
        import re

        result = {
            "genres": [],
            "themes": []
        }

        valid_genre_names = set(g.get('name', '').lower() for g in self.genres_data if g.get('name'))
        valid_theme_names = set(t.get('name', '').lower() for t in self.themes_data if t.get('name'))

        genre_pattern = r'"name":\s*"([^"]+)",\s*"decision":\s*"([^"]+)",\s*"explanation":\s*"([^"]+(?:\\"[^"]*|[^"]+)*)"'

        # Извлекаем секцию genres
        genres_section = re.search(r'"genres"\s*:\s*\[(.*?)\](?=,?\s*"themes")', json_text, re.DOTALL)
        if genres_section:
            genres_text = genres_section.group(1)
            for match in re.finditer(genre_pattern, genres_text, re.DOTALL):
                name = match.group(1)
                if name.lower() in valid_genre_names:
                    decision = match.group(2)
                    explanation = match.group(3).replace('\\"', '"').replace('\\n', ' ')
                    result["genres"].append({
                        "name": name,
                        "decision": decision,
                        "explanation": explanation
                    })

        # Извлекаем секцию themes
        themes_section = re.search(r'"themes"\s*:\s*\[(.*?)\]\s*}', json_text, re.DOTALL)
        if themes_section:
            themes_text = themes_section.group(1)
            for match in re.finditer(genre_pattern, themes_text, re.DOTALL):
                name = match.group(1)
                if name.lower() in valid_theme_names:
                    decision = match.group(2)
                    explanation = match.group(3).replace('\\"', '"').replace('\\n', ' ')
                    result["themes"].append({
                        "name": name,
                        "decision": decision,
                        "explanation": explanation
                    })

        return result

    def extract_matching_genres_themes(self, analysis: Dict) -> Tuple[List[str], List[str]]:
        """
        Извлекает из анализа список жанров и тем с решением YES.
        """
        valid_genre_names = set(g.get('name', '').lower() for g in self.genres_data if g.get('name'))
        valid_theme_names = set(t.get('name', '').lower() for t in self.themes_data if t.get('name'))

        matching_genres = []
        matching_themes = []

        for genre in analysis.get('genres', []):
            name = genre.get('name', '')
            decision = genre.get('decision', '').upper()

            if name.lower() in valid_genre_names:
                if decision == 'YES':
                    matching_genres.append(name)
            else:
                self.log_message(f'      ⚠ Пропущен неизвестный жанр: {name}', 'WARNING', verbose_only=True)

        for theme in analysis.get('themes', []):
            name = theme.get('name', '')
            decision = theme.get('decision', '').upper()

            if name.lower() in valid_theme_names:
                if decision == 'YES':
                    matching_themes.append(name)
            else:
                self.log_message(f'      ⚠ Пропущена неизвестная тема: {name}', 'WARNING', verbose_only=True)

        return matching_genres, matching_themes

    def assign_genres_to_game(self, game: Game, genre_names: List[str]) -> int:
        """Присваивает жанры игре."""
        assigned_count = 0
        genres_to_add = []

        for genre_name in genre_names:
            genre_lower = genre_name.lower()

            if genre_lower in self.genre_cache:
                genre = self.genre_cache[genre_lower]
            else:
                genre = Genre.objects.create(name=genre_name)
                self.genre_cache[genre_lower] = genre
                self.log_message(f'      📁 Создан новый жанр: {genre_name}', verbose_only=True)

            if not game.genres.filter(id=genre.id).exists():
                genres_to_add.append(genre)
                assigned_count += 1

        if genres_to_add:
            game.genres.add(*genres_to_add)

        return assigned_count

    def assign_themes_to_game(self, game: Game, theme_names: List[str]) -> int:
        """Присваивает темы игре."""
        assigned_count = 0
        themes_to_add = []

        for theme_name in theme_names:
            theme_lower = theme_name.lower()

            if theme_lower in self.theme_cache:
                theme = self.theme_cache[theme_lower]
            else:
                theme = Theme.objects.create(name=theme_name)
                self.theme_cache[theme_lower] = theme
                self.log_message(f'      📁 Создана новая тема: {theme_name}', verbose_only=True)

            if not game.themes.filter(id=theme.id).exists():
                themes_to_add.append(theme)
                assigned_count += 1

        if themes_to_add:
            game.themes.add(*themes_to_add)

        return assigned_count

    def generate_report_entry(self, game: Game, analysis: Dict, genres: List[str], themes: List[str]) -> str:
        """Генерирует запись для отчета."""
        report = []
        report.append(f'GAME: {game.name}')
        report.append(f'ID: {game.id}')
        report.append(f'IGDB ID: {game.igdb_id}')
        report.append('-' * 80)

        report.append('GENRES ANALYSIS:')
        for genre in analysis.get('genres', []):
            report.append(f'  • {genre["name"]}: {genre["decision"]}')
            report.append(f'    Explanation: {genre["explanation"]}')

        report.append('\nTHEMES ANALYSIS:')
        for theme in analysis.get('themes', []):
            report.append(f'  • {theme["name"]}: {theme["decision"]}')
            report.append(f'    Explanation: {theme["explanation"]}')

        report.append('\n=== SUMMARY ===')
        report.append(f'Matching genres ({len(genres)}): {", ".join(genres) if genres else "None"}')
        report.append(f'Matching themes ({len(themes)}): {", ".join(themes) if themes else "None"}')
        report.append('=' * 80 + '\n')

        return '\n'.join(report)

    def generate_summary_entry(self, game: Game, genres: List[str], themes: List[str]) -> str:
        """Генерирует краткую запись для файла-резюме."""
        summary = []
        summary.append(f'GAME: {game.name}')
        summary.append(f'Matching genres ({len(genres)}): {", ".join(genres) if genres else "None"}')
        summary.append(f'Matching themes ({len(themes)}): {", ".join(themes) if themes else "None"}')
        summary.append('-' * 80)
        summary.append('')
        return '\n'.join(summary)

    def log_error(self, error_file, game: Game, error_type: str, error_message: str,
                  prompt: Optional[str] = None, raw_response: Optional[str] = None,
                  exception: Optional[Exception] = None):
        """Записывает ошибку в файл."""
        error_file.write('=' * 80 + '\n')
        error_file.write(f'ERROR ENTRY\n')
        error_file.write(f'Timestamp: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
        error_file.write(f'Game ID: {game.id}\n')
        error_file.write(f'Game Name: {game.name}\n')
        error_file.write(f'IGDB ID: {game.igdb_id}\n')
        error_file.write(f'Error Type: {error_type}\n')
        error_file.write(f'Error Message: {error_message}\n')
        error_file.write('-' * 80 + '\n')

        if prompt:
            error_file.write('PROMPT SENT TO GIGACHAT:\n')
            error_file.write(prompt)
            error_file.write('\n' + '-' * 80 + '\n')

        if raw_response:
            error_file.write('RAW RESPONSE FROM GIGACHAT:\n')
            error_file.write(raw_response)
            error_file.write('\n' + '-' * 80 + '\n')

        if exception:
            error_file.write('EXCEPTION TRACEBACK:\n')
            error_file.write(traceback.format_exc())
            error_file.write('\n' + '-' * 80 + '\n')

        error_file.write('\n')
        error_file.flush()

    def process_games(self, games: models.QuerySet, options: Dict[str, Any],
                      output_file_path: str, error_file_path: str,
                      summary_file_path: str, resume_file_path: str,
                      processed_games: set, dry_run: bool) -> None:
        """
        Обработка игр: анализ и присвоение жанров/тем.
        """
        # Подготовка файлов
        write_mode = 'a' if options.get('resume') else 'w'

        with open(output_file_path, write_mode, encoding='utf-8') as report_file:
            with open(error_file_path, write_mode, encoding='utf-8') as error_file:
                with open(summary_file_path, write_mode, encoding='utf-8') as summary_file:

                    if not options.get('resume'):
                        error_file.write('=' * 80 + '\n')
                        error_file.write('ERROR LOG - GENRES AND THEMES ASSIGNMENT\n')
                        error_file.write(f'Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
                        error_file.write('=' * 80 + '\n\n')

                        report_file.write('=' * 80 + '\n')
                        report_file.write('GENRES AND THEMES ASSIGNMENT REPORT\n')
                        report_file.write(f'Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
                        report_file.write('=' * 80 + '\n\n')

                        summary_file.write('=' * 80 + '\n')
                        summary_file.write('GENRES AND THEMES ASSIGNMENT SUMMARY\n')
                        summary_file.write(f'Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
                        summary_file.write('=' * 80 + '\n\n')

                    processed_count = 0
                    interrupted_after_current = False

                    for idx, game in enumerate(games, 1):
                        self.current_game = game
                        self.current_game_index = idx

                        # Проверка прерывания перед началом новой игры
                        if interrupted_after_current:
                            break

                        # Проверка на уже обработанные
                        if game.id in processed_games:
                            processed_count += 1
                            self.progress_bar.update(processed_count, f"⏭️ Пропуск (уже обработана)")
                            continue

                        # Обновляем прогресс-бар с текущей игрой
                        self.progress_bar.update(processed_count, f"🔄 {game.name[:40]}...")

                        # Проверка токена
                        if not self.ensure_valid_token():
                            error_msg = "Не удалось обновить токен"
                            self.games_failed += 1
                            self.log_error(error_file, game, "Token Error", error_msg)
                            processed_count += 1
                            self.progress_bar.update(processed_count, f"❌ Ошибка: {game.name[:30]}...")
                            continue

                        try:
                            # Анализ игры
                            analysis, prompt, raw_response = self.analyze_game_genres_themes(game)

                            if not analysis:
                                error_msg = "Не удалось выполнить анализ"
                                self.games_failed += 1
                                self.log_error(error_file, game, "Analysis Error", error_msg, prompt, raw_response)
                                processed_count += 1
                                self.progress_bar.update(processed_count, f"❌ Ошибка: {game.name[:30]}...")
                                continue

                            # Извлекаем жанры и темы
                            matching_genres, matching_themes = self.extract_matching_genres_themes(analysis)

                            # Записываем отчеты
                            report_entry = self.generate_report_entry(game, analysis, matching_genres, matching_themes)
                            report_file.write(report_entry)
                            report_file.flush()

                            summary_entry = self.generate_summary_entry(game, matching_genres, matching_themes)
                            summary_file.write(summary_entry)
                            summary_file.flush()

                            # Выводим подробности если verbose
                            if self.verbose:
                                # Временно убираем прогресс-бар для вывода
                                self.stdout.write('\n', ending='')
                                self.stdout.write(f'  🎭 Найдено жанров: {len(matching_genres)}')
                                for genre in matching_genres:
                                    self.stdout.write(f'     - {genre}')
                                self.stdout.write(f'  🎨 Найдено тем: {len(matching_themes)}')
                                for theme in matching_themes:
                                    self.stdout.write(f'     - {theme}')

                            # Сохраняем в БД
                            if not dry_run:
                                try:
                                    genres_assigned = self.assign_genres_to_game(game, matching_genres)
                                    themes_assigned = self.assign_themes_to_game(game, matching_themes)

                                    game.update_materialized_vectors(force=True)
                                    game.update_cached_counts(force=True)

                                    self.games_processed += 1

                                    # Сохраняем состояние ТОЛЬКО после успешного сохранения
                                    self.save_resume_state(resume_file_path, game.id)

                                    if self.verbose:
                                        self.stdout.write(
                                            f'  ✓ Сохранено: {genres_assigned} жанров, {themes_assigned} тем')

                                except Exception as db_error:
                                    error_msg = f"Ошибка при сохранении в БД: {str(db_error)}"
                                    self.games_failed += 1
                                    self.log_error(error_file, game, "Database Error", error_msg, prompt, raw_response,
                                                   db_error)
                            else:
                                self.games_processed += 1
                                if self.verbose:
                                    self.stdout.write(f'  🔍 Dry run: изменения не применены')

                            # Увеличиваем счетчик обработанных и обновляем прогресс-бар
                            processed_count += 1
                            self.progress_bar.update(processed_count, f"✅ {game.name[:30]}...")

                        except Exception as e:
                            error_msg = f"Необработанная ошибка: {str(e)}"
                            self.games_failed += 1
                            self.log_error(error_file, game, "Unexpected Error", error_msg, exception=e)
                            processed_count += 1
                            self.progress_bar.update(processed_count, f"❌ Ошибка: {game.name[:30]}...")

                        # После завершения обработки игры проверяем флаг прерывания
                        if self.interrupted:
                            # Устанавливаем флаг, что прерывание произошло после текущей игры
                            interrupted_after_current = True
                            # Выводим сообщение
                            self.stdout.write('\n', ending='')
                            self.stdout.flush()
                            self.stdout.write(self.style.WARNING('✓ Текущая игра завершена, прерывание...'))
                            self.stdout.flush()
                            # Обновляем прогресс-бар с реальным количеством
                            self.progress_bar.update(processed_count,
                                                     f"⏸️ Остановлено на {processed_count}/{self.total_games}")
                            # Выходим из цикла (не вызываем finish)
                            break

                        # Задержка между запросами
                        time.sleep(2)

                    # НЕ вызываем finish здесь - это будет сделано в handle
                    # Просто обновляем прогресс-бар до финального состояния если не было прерывания
                    if not interrupted_after_current and not self.interrupted and processed_count == self.total_games:
                        self.progress_bar.update(processed_count,
                                                 f"✅ {self.games_processed} успешно | ❌ {self.games_failed} ошибок")

    def show_summary(self) -> None:
        """Вывод статистики выполнения"""
        elapsed_time = time.time() - self.start_time

        # Форматируем время
        if elapsed_time < 60:
            elapsed_str = f"{elapsed_time:.1f}s"
        elif elapsed_time < 3600:
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            elapsed_str = f"{minutes}m {seconds}s"
        else:
            hours = int(elapsed_time // 3600)
            minutes = int((elapsed_time % 3600) // 60)
            elapsed_str = f"{hours}h {minutes}m"

        # Выводим статистику
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'✅ Успешно обработано: {self.games_processed}')
        self.stdout.write(f'❌ Не удалось: {self.games_failed}')
        self.stdout.write(f'⏱️  Время: {elapsed_str}')
        if self.interrupted:
            self.stdout.write(self.style.WARNING('⚠ Работа прервана пользователем'))
        self.stdout.write('=' * 60)
