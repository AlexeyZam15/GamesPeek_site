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

        # Настройка обработчиков сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """
        Обработчик сигналов прерывания (Ctrl+C, SIGTERM).
        """
        self.stdout.write('\n')
        self.stdout.write(self.style.WARNING('⚠ Получен сигнал прерывания...'))
        self.stdout.write(self.style.WARNING('⚠ Завершение работы после сохранения текущего состояния...'))
        self.interrupted = True

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

    def handle(self, *args, **options):
        self.start_time = time.time()

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
            self.stdout.write(
                self.style.SUCCESS(f'📋 Режим возобновления: пропускаем {len(processed_games)} уже обработанных игр'))

        if not games.exists():
            self.stdout.write(self.style.WARNING('Нет игр для обработки.'))
            return

        total = games.count()
        self.total_games = total
        self.stdout.write(f'📊 Игр к обработке: {total}')
        self.stdout.write(f'📁 Выходной файл: {output_file_path}')
        self.stdout.write(f'📁 Файл ошибок: {error_file_path}')
        self.stdout.write(f'📁 Краткий файл: {summary_file_path}')
        self.stdout.write(f'📁 Файл состояния: {resume_file_path}')

        if dry_run:
            self.stdout.write(self.style.WARNING('🔍 РЕЖИМ DRY RUN - изменения в БД НЕ будут применены\n'))
        else:
            self.stdout.write('\n')

        if resume and processed_games:
            self.stdout.write(self.style.WARNING(f'📋 Продолжаем с {len(processed_games) + 1} игры\n'))

        self.process_games(games, options, output_file_path, error_file_path,
                           summary_file_path, resume_file_path, processed_games, dry_run)
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
            self.stdout.write(self.style.WARNING(f'⚠ Не удалось загрузить состояние: {e}'))
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
            self.stdout.write(self.style.WARNING(f'⚠ Не удалось сохранить состояние: {e}'))

    def setup_api(self) -> bool:
        """Настройка API GigaChat"""
        self.auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', None)

        if not self.auth_key:
            self.stdout.write(self.style.ERROR(
                'GIGACHAT_AUTH_KEY должен быть указан в settings.py'
            ))
            return False

        self.stdout.write(self.style.SUCCESS(f'✅ GigaChat настроен\n'))
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

            self.stdout.write('   Получение токена GigaChat...')
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

                self.stdout.write(self.style.SUCCESS(f'   ✅ Токен получен, истекает через {expires_in} секунд'))
                return True
            else:
                self.stdout.write(self.style.ERROR(f'   ❌ Ошибка получения токена: {response.status_code}'))
                return False

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {str(e)}'))
            return False

    def ensure_valid_token(self) -> bool:
        """Проверка и обновление токена при необходимости"""
        if not self.access_token or time.time() >= self.token_expires_at:
            self.stdout.write('   Токен истек, обновление...')
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
                self.stdout.write(
                    self.style.SUCCESS(f'✅ Загружено жанров: {len(self.genres_data)}, тем: {len(self.themes_data)}'))
            else:
                self.stdout.write(self.style.ERROR(f'❌ Файл должен содержать объект с полями genres и themes'))

        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f'❌ Файл {file_path} не найден'))
            self.genres_data = []
            self.themes_data = []
        except json.JSONDecodeError as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка парсинга JSON: {e}'))
            self.genres_data = []
            self.themes_data = []
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка: {str(e)}'))
            self.genres_data = []
            self.themes_data = []

    def load_existing_genres_themes(self):
        """Загрузка существующих жанров и тем из БД в кэш"""
        for genre in Genre.objects.all():
            self.genre_cache[genre.name.lower()] = genre

        for theme in Theme.objects.all():
            self.theme_cache[theme.name.lower()] = theme

        self.stdout.write(
            self.style.SUCCESS(f'✅ Загружено из БД: {len(self.genre_cache)} жанров, {len(self.theme_cache)} тем'))

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
            self.stdout.write(self.style.SUCCESS(f'✓ Найдено точное совпадение: {exact_match.first().name}'))
            return exact_match

        name_match = Game.objects.filter(name__icontains=game_name).order_by('-rating_count')

        if name_match.exists():
            best_match = name_match.first()
            self.stdout.write(
                self.style.WARNING(
                    f'⚠ Нет точного совпадения для "{game_name}". '
                    f'Использую: "{best_match.name}"'
                )
            )
            return Game.objects.filter(id=best_match.id)

        self.stdout.write(self.style.ERROR(f'✗ Игры не найдены'))
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
                        # Извлекаем JSON из ответа (на случай если есть лишний текст)
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
                            self.stdout.write(self.style.WARNING(f'  ⚠ Попытка восстановить некорректный JSON...'))

                            # Восстанавливаем JSON: исправляем незакрытые объекты
                            lines = json_text.split('\n')
                            bracket_count = 0

                            for line in lines:
                                bracket_count += line.count('{')
                                bracket_count -= line.count('}')

                            fixed_json = json_text

                            # Проверяем, не пропущена ли закрывающая скобка в конце
                            if bracket_count > 0:
                                fixed_json += '}' * bracket_count

                            # Проверяем, не пропущена ли запятая перед закрывающей скобкой
                            fixed_json = re.sub(r',\s*}', '}', fixed_json)
                            fixed_json = re.sub(r',\s*]', ']', fixed_json)

                            try:
                                data = json.loads(fixed_json)
                                self.stdout.write(self.style.SUCCESS(f'  ✅ JSON успешно восстановлен'))
                            except json.JSONDecodeError as e2:
                                self.stdout.write(
                                    self.style.WARNING(f'  ⚠ Извлечение данных через регулярные выражения...'))
                                data = self.extract_genres_themes_with_regex(json_text)

                                if not data:
                                    self.stdout.write(self.style.ERROR(f'  ✗ Не удалось восстановить JSON: {e2}'))
                                    return None, prompt, raw_response

                        return data, prompt, raw_response

                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'  ✗ Ошибка парсинга JSON: {e}'))
                        return None, prompt, raw_response
                else:
                    return None, prompt, raw_response
            else:
                if response.status_code == 401:
                    self.access_token = None
                self.stdout.write(self.style.ERROR(f'  ✗ Ошибка API: {response.status_code}'))
                return None, None, raw_response

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  ✗ Ошибка при анализе: {str(e)}'))
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

        # Паттерн для извлечения информации о жанре
        genre_pattern = r'"name":\s*"([^"]+)",\s*"decision":\s*"([^"]+)",\s*"explanation":\s*"([^"]+(?:\\"[^"]*|[^"]+)*)"'

        # Извлекаем секцию genres
        genres_section = re.search(r'"genres"\s*:\s*\[(.*?)\](?=,?\s*"themes")', json_text, re.DOTALL)
        if genres_section:
            genres_text = genres_section.group(1)
            for match in re.finditer(genre_pattern, genres_text, re.DOTALL):
                name = match.group(1)
                decision = match.group(2)
                explanation = match.group(3)
                explanation = explanation.replace('\\"', '"').replace('\\n', ' ')
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
                decision = match.group(2)
                explanation = match.group(3)
                explanation = explanation.replace('\\"', '"').replace('\\n', ' ')
                result["themes"].append({
                    "name": name,
                    "decision": decision,
                    "explanation": explanation
                })

        # Если ничего не нашли через регулярки, пробуем другой подход
        if not result["genres"] and not result["themes"]:
            for section_name in ["genres", "themes"]:
                items = []
                section_pattern = rf'"{section_name}"\s*:\s*\[(.*?)\]'
                section_match = re.search(section_pattern, json_text, re.DOTALL)

                if section_match:
                    section_text = section_match.group(1)
                    # Разделяем на отдельные объекты
                    objects = re.split(r'}\s*,?\s*{', section_text)

                    for obj in objects:
                        if not obj.startswith('{'):
                            obj = '{' + obj
                        if not obj.endswith('}'):
                            obj = obj + '}'

                        name_match = re.search(r'"name":\s*"([^"]+)"', obj)
                        decision_match = re.search(r'"decision":\s*"([^"]+)"', obj)
                        explanation_match = re.search(r'"explanation":\s*"([^"]+(?:\\"[^"]*|[^"]+)*)"', obj, re.DOTALL)

                        if name_match and decision_match:
                            item = {
                                "name": name_match.group(1),
                                "decision": decision_match.group(1).upper()
                            }
                            if explanation_match:
                                explanation = explanation_match.group(1).replace('\\"', '"').replace('\\n', ' ')
                                item["explanation"] = explanation
                            else:
                                item["explanation"] = "No explanation provided"
                            items.append(item)

                    if section_name == "genres":
                        result["genres"] = items
                    else:
                        result["themes"] = items

        if not result["genres"]:
            result["genres"] = []

        if not result["themes"]:
            result["themes"] = []

        return result

    def extract_matching_genres_themes(self, analysis: Dict) -> Tuple[List[str], List[str]]:
        """
        Извлекает из анализа список жанров и тем с решением YES.
        Возвращает tuple (список_жанров, список_тем)
        """
        # Получаем допустимые имена жанров и тем из исходных данных
        valid_genre_names = set(g.get('name', '').lower() for g in self.genres_data if g.get('name'))
        valid_theme_names = set(t.get('name', '').lower() for t in self.themes_data if t.get('name'))

        matching_genres = []
        matching_themes = []

        # Обрабатываем жанры
        for genre in analysis.get('genres', []):
            name = genre.get('name', '')
            decision = genre.get('decision', '').upper()

            # Проверяем, что жанр есть в допустимом списке
            if name.lower() in valid_genre_names:
                if decision == 'YES':
                    matching_genres.append(name)
            else:
                self.stdout.write(self.style.WARNING(f'      ⚠ Пропущен неизвестный жанр: {name}'))

        # Обрабатываем темы
        for theme in analysis.get('themes', []):
            name = theme.get('name', '')
            decision = theme.get('decision', '').upper()

            # Проверяем, что тема есть в допустимом списке
            if name.lower() in valid_theme_names:
                if decision == 'YES':
                    matching_themes.append(name)
            else:
                self.stdout.write(self.style.WARNING(f'      ⚠ Пропущена неизвестная тема: {name}'))

        return matching_genres, matching_themes

    def assign_genres_to_game(self, game: Game, genre_names: List[str]) -> int:
        """
        Присваивает жанры игре через ManyToMany поле. Создает новые жанры при необходимости.
        Возвращает количество присвоенных жанров.
        """
        assigned_count = 0
        genres_to_add = []

        for genre_name in genre_names:
            genre_lower = genre_name.lower()

            # Проверяем, существует ли жанр в кэше
            if genre_lower in self.genre_cache:
                genre = self.genre_cache[genre_lower]
            else:
                # Создаем новый жанр
                genre = Genre.objects.create(name=genre_name)
                self.genre_cache[genre_lower] = genre
                self.stdout.write(f'      📁 Создан новый жанр: {genre_name}')

            # Проверяем, не присвоен ли уже этот жанр игре
            if not game.genres.filter(id=genre.id).exists():
                genres_to_add.append(genre)
                assigned_count += 1

        # Массовое добавление жанров
        if genres_to_add:
            game.genres.add(*genres_to_add)

        return assigned_count

    def assign_themes_to_game(self, game: Game, theme_names: List[str]) -> int:
        """
        Присваивает темы игре через ManyToMany поле. Создает новые темы при необходимости.
        Возвращает количество присвоенных тем.
        """
        assigned_count = 0
        themes_to_add = []

        for theme_name in theme_names:
            theme_lower = theme_name.lower()

            # Проверяем, существует ли тема в кэше
            if theme_lower in self.theme_cache:
                theme = self.theme_cache[theme_lower]
            else:
                # Создаем новую тему
                theme = Theme.objects.create(name=theme_name)
                self.theme_cache[theme_lower] = theme
                self.stdout.write(f'      📁 Создана новая тема: {theme_name}')

            # Проверяем, не присвоена ли уже эта тема игре
            if not game.themes.filter(id=theme.id).exists():
                themes_to_add.append(theme)
                assigned_count += 1

        # Массовое добавление тем
        if themes_to_add:
            game.themes.add(*themes_to_add)

        return assigned_count

    def generate_report_entry(self, game: Game, analysis: Dict, genres: List[str], themes: List[str]) -> str:
        """
        Генерирует запись для отчета по игре.
        """
        report = []
        report.append(f'GAME: {game.name}')
        report.append(f'ID: {game.id}')
        report.append(f'IGDB ID: {game.igdb_id}')
        report.append('-' * 80)

        # Жанры с подробными объяснениями
        report.append('GENRES ANALYSIS:')
        for genre in analysis.get('genres', []):
            report.append(f'  • {genre["name"]}: {genre["decision"]}')
            report.append(f'    Explanation: {genre["explanation"]}')

        # Темы с подробными объяснениями
        report.append('\nTHEMES ANALYSIS:')
        for theme in analysis.get('themes', []):
            report.append(f'  • {theme["name"]}: {theme["decision"]}')
            report.append(f'    Explanation: {theme["explanation"]}')

        # Краткое резюме
        report.append('\n=== SUMMARY ===')
        report.append(f'Matching genres ({len(genres)}): {", ".join(genres) if genres else "None"}')
        report.append(f'Matching themes ({len(themes)}): {", ".join(themes) if themes else "None"}')
        report.append('=' * 80 + '\n')

        return '\n'.join(report)

    def generate_summary_entry(self, game: Game, genres: List[str], themes: List[str]) -> str:
        """
        Генерирует краткую запись для файла-резюме.
        """
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
        """
        Записывает ошибку в файл с полной информацией.
        """
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
        # Подготовка файла для отчета
        if not options.get('resume') and os.path.exists(output_file_path):
            os.remove(output_file_path)
            self.stdout.write(f'🗑️  Удален существующий файл: {output_file_path}')

        # Подготовка файла для ошибок
        if not options.get('resume') and os.path.exists(error_file_path):
            os.remove(error_file_path)
            self.stdout.write(f'🗑️  Удален существующий файл: {error_file_path}')

        # Подготовка файла для краткого резюме
        if not options.get('resume') and os.path.exists(summary_file_path):
            os.remove(summary_file_path)
            self.stdout.write(f'🗑️  Удален существующий файл: {summary_file_path}')

        # Определяем режим открытия файлов
        write_mode = 'a' if options.get('resume') else 'w'

        with open(output_file_path, write_mode, encoding='utf-8') as report_file:
            with open(error_file_path, write_mode, encoding='utf-8') as error_file:
                with open(summary_file_path, write_mode, encoding='utf-8') as summary_file:

                    # Если это новый запуск, пишем заголовки
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

                    for idx, game in enumerate(games, 1):
                        self.current_game = game
                        self.current_game_index = idx

                        # Проверяем, была ли игра уже обработана (для режима возобновления)
                        if game.id in processed_games:
                            self.stdout.write(f'\n[{idx}/{self.total_games}] Пропускаем (уже обработана): {game.name}')
                            continue

                        # Проверяем сигнал прерывания
                        if self.interrupted:
                            self.stdout.write(self.style.WARNING('\n⚠ Прерывание работы...'))
                            self.stdout.write(
                                self.style.WARNING(f'⚠ Обработано {self.games_processed} игр, сохранение состояния...'))
                            break

                        self.stdout.write(f'\n[{idx}/{self.total_games}] Обработка: {game.name}')

                        # Проверка токена
                        if not self.ensure_valid_token():
                            error_msg = "Не удалось обновить токен"
                            self.stdout.write(self.style.ERROR(f'  ✗ {error_msg}'))
                            self.games_failed += 1
                            self.log_error(error_file, game, "Token Error", error_msg)
                            continue

                        try:
                            # Анализ игры через GigaChat
                            analysis, prompt, raw_response = self.analyze_game_genres_themes(game)

                            if not analysis:
                                error_msg = "Не удалось выполнить анализ"
                                self.stdout.write(self.style.ERROR(f'  ✗ {error_msg}'))
                                self.games_failed += 1
                                self.log_error(error_file, game, "Analysis Error", error_msg, prompt, raw_response)
                                continue

                            # Извлекаем подходящие жанры и темы
                            matching_genres, matching_themes = self.extract_matching_genres_themes(analysis)

                            # Генерируем запись для отчета
                            report_entry = self.generate_report_entry(game, analysis, matching_genres, matching_themes)
                            report_file.write(report_entry)
                            report_file.flush()

                            # Генерируем краткую запись для резюме
                            summary_entry = self.generate_summary_entry(game, matching_genres, matching_themes)
                            summary_file.write(summary_entry)
                            summary_file.flush()

                            # Показываем найденные жанры и темы
                            self.stdout.write(f'  🎭 Найдено жанров: {len(matching_genres)}')
                            for genre in matching_genres:
                                self.stdout.write(f'     - {genre}')

                            self.stdout.write(f'  🎨 Найдено тем: {len(matching_themes)}')
                            for theme in matching_themes:
                                self.stdout.write(f'     - {theme}')

                            # Если не dry-run, сохраняем в БД
                            if not dry_run:
                                try:
                                    genres_assigned = self.assign_genres_to_game(game, matching_genres)
                                    themes_assigned = self.assign_themes_to_game(game, matching_themes)

                                    # Обновляем материализованные векторы после добавления жанров и тем
                                    game.update_materialized_vectors(force=True)
                                    game.update_cached_counts(force=True)

                                    self.stdout.write(self.style.SUCCESS(
                                        f'  ✓ Сохранено в БД: {genres_assigned} жанров, {themes_assigned} тем'
                                    ))
                                    self.games_processed += 1

                                    # Сохраняем состояние для возобновления
                                    self.save_resume_state(resume_file_path, game.id)

                                except Exception as db_error:
                                    error_msg = f"Ошибка при сохранении в БД: {str(db_error)}"
                                    self.stdout.write(self.style.ERROR(f'  ✗ {error_msg}'))
                                    self.games_failed += 1
                                    self.log_error(error_file, game, "Database Error", error_msg, prompt, raw_response,
                                                   db_error)
                            else:
                                self.stdout.write(self.style.WARNING(f'  🔍 Dry run: изменения не применены'))
                                self.games_processed += 1

                        except Exception as e:
                            error_msg = f"Необработанная ошибка: {str(e)}"
                            self.stdout.write(self.style.ERROR(f'  ✗ {error_msg}'))
                            self.games_failed += 1
                            self.log_error(error_file, game, "Unexpected Error", error_msg, exception=e)

                        # Задержка между запросами для соблюдения лимитов API
                        if not self.interrupted:
                            time.sleep(2)

    def show_summary(self) -> None:
        """Вывод статистики выполнения"""
        elapsed_time = time.time() - self.start_time

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'✅ Успешно обработано: {self.games_processed}')
        self.stdout.write(f'❌ Не удалось: {self.games_failed}')
        self.stdout.write(f'⏱️  Время: {elapsed_time:.2f} секунд')
        if self.interrupted:
            self.stdout.write(self.style.WARNING('⚠ Работа прервана пользователем'))
        self.stdout.write('=' * 60)