"""
Команда Django для генерации описаний геймплея для игр без rawg_description
с использованием GigaChat API (бесплатный тариф, работает в России).
"""

import time
import logging
import requests
import urllib3
import uuid
import os
import json
from typing import Optional, Dict, Any
from django.core.management.base import BaseCommand
from django.db import models
from django.conf import settings

from games.models import Game

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Команда Django для генерации описаний геймплея через GigaChat API.
    """

    help = 'Генерирует английские описания геймплея для игр без rawg_description через GigaChat'

    def add_arguments(self, parser):
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
        parser.add_argument(
            '--genres-only',
            action='store_true',
            help='Только определить жанры и темы без генерации полного описания.',
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
        self.genres_only_mode = False
        self.auth_url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        self.api_url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        self.genres_data = []
        self.themes_data = []

    def handle(self, *args, **options):
        self.start_time = time.time()
        self.genres_only_mode = options.get('genres_only', False)

        if not self.setup_api():
            return

        self.load_genre_theme_descriptions()

        output_file_path = options.get('output_file')
        games = self.get_games_to_process(options)

        if not games.exists():
            self.stdout.write(self.style.WARNING('Нет игр для обработки.'))
            return

        total = games.count()
        self.stdout.write(f'📊 Игр к обработке: {total}')
        self.stdout.write(f'📁 Выходной файл: {output_file_path}')
        if self.genres_only_mode:
            self.stdout.write(self.style.WARNING('🎯 Режим: ТОЛЬКО ОПРЕДЕЛЕНИЕ ЖАНРОВ И ТЕМ\n'))
        else:
            self.stdout.write(f'\n')

        if options.get('dry_run'):
            self.display_dry_run(games)
            return

        self.process_games(games, options)
        self.show_summary()

    def setup_api(self) -> bool:
        self.auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', None)

        if not self.auth_key:
            self.stdout.write(self.style.ERROR(
                'GIGACHAT_AUTH_KEY должен быть указан в settings.py'
            ))
            return False

        self.stdout.write(self.style.SUCCESS(f'✅ GigaChat настроен\n'))
        return self.get_access_token()

    def get_access_token(self) -> bool:
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
        if not self.access_token or time.time() >= self.token_expires_at:
            self.stdout.write('   Токен истек, обновление...')
            return self.get_access_token()
        return True

    def load_genre_theme_descriptions(self):
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

    def get_games_to_process(self, options: Dict[str, Any]) -> models.QuerySet:
        game_name = options.get('name')
        force = options.get('force', False)

        if game_name:
            return self.get_specific_game(game_name)

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

    def display_dry_run(self, games: models.QuerySet) -> None:
        if self.genres_only_mode:
            self.stdout.write(self.style.WARNING('🔍 РЕЖИМ DRY RUN - Только определение жанров и тем\n'))
        else:
            self.stdout.write(self.style.WARNING('🔍 РЕЖИМ DRY RUN - Описания генерироваться не будут\n'))

        for idx, game in enumerate(games, 1):
            self.stdout.write(f'{idx:3}. {game.name}')

        self.stdout.write(f'\n✅ Будет обработано {games.count()} игр.')

    def process_games(self, games: models.QuerySet, options: Dict[str, Any]) -> None:
        output_file_path = options.get('output_file')
        force = options.get('force', False)

        if os.path.exists(output_file_path):
            os.remove(output_file_path)
            self.stdout.write(f'🗑️  Удален существующий файл: {output_file_path}')

        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            output_file.write('=' * 80 + '\n')
            if self.genres_only_mode:
                output_file.write('GENRES AND THEMES ANALYSIS WITH DETAILED EXPLANATIONS\n')
                output_file.write('Generated by GigaChat with per-item reasoning\n')
            else:
                output_file.write('GAMEPLAY DESCRIPTIONS GENERATED BY GIGACHAT\n')
            output_file.write(f'Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
            output_file.write('=' * 80 + '\n\n')

            for idx, game in enumerate(games, 1):
                self.stdout.write(f'\n[{idx}/{games.count()}] Обработка: {game.name}')

                if not force and game.rawg_description and not self.genres_only_mode:
                    self.stdout.write(self.style.WARNING(f'  ⏭ Пропуск (уже есть описание)'))
                    output_file.write(f'GAME: {game.name}\n')
                    output_file.write(f'ID: {game.id}\n')
                    output_file.write('-' * 80 + '\n')
                    output_file.write(f'[SKIPPED] Already has description\n')
                    output_file.write('=' * 80 + '\n\n')
                    continue

                if not self.ensure_valid_token():
                    self.stdout.write(self.style.ERROR(f'  ✗ Не удалось обновить токен'))
                    self.games_failed += 1
                    continue

                try:
                    if self.genres_only_mode:
                        result, prompt = self.generate_genres_analysis(game)
                    else:
                        result, prompt = self.generate_description(game)

                    if result:
                        game.rawg_description = result
                        game.save(update_fields=['rawg_description'])

                        output_file.write(f'GAME: {game.name}\n')
                        output_file.write(f'ID: {game.id}\n')
                        output_file.write(f'IGDB ID: {game.igdb_id}\n')
                        output_file.write('-' * 80 + '\n')
                        output_file.write(f'PROMPT SENT TO GIGACHAT:\n')
                        output_file.write(f'{prompt}\n')
                        output_file.write('-' * 80 + '\n')
                        output_file.write(f'RESPONSE FROM GIGACHAT:\n')
                        output_file.write(f'{result}\n')
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
                        output_file.write(f'[FAILED]\n')
                        output_file.write('=' * 80 + '\n\n')
                        output_file.flush()

                except Exception as e:
                    self.games_failed += 1
                    self.stdout.write(self.style.ERROR(f'  ✗ Ошибка: {str(e)}'))

                time.sleep(2)

    def build_compact_game_context(self, game: Game) -> str:
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
        return f"""Analyze the game "{game_name}" and determine which genres and themes from the provided lists apply.

    Game Information:
    {context}

    Here are ALL genres and themes with their definitions in JSON format:

    {json.dumps({"genres": self.genres_data, "themes": self.themes_data}, ensure_ascii=False, indent=2)}

    You MUST output your analysis in EXACT JSON format as shown below.
    Do NOT add any additional text before or after the JSON.
    Do NOT add any Conclusion section.

    Output format:
    {{
      "genres": [
        {{"name": "Genre Name 1", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}},
        {{"name": "Genre Name 2", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}}
      ],
      "themes": [
        {{"name": "Theme Name 1", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}},
        {{"name": "Theme Name 2", "decision": "YES/NO/MAYBE", "explanation": "2-3 detailed sentences explaining WHY"}}
      ]
    }}

    REMEMBER:
    - EVERY genre and theme from the JSON above must appear in your output
    - EVERY explanation must be specific to this game
    - Output ONLY valid JSON, no other text
    - Use only "YES", "NO", or "MAYBE" for decision field"""

    def generate_genres_analysis(self, game: Game) -> tuple[Optional[str], Optional[str]]:
        """
        Генерирует анализ жанров и тем с подробными объяснениями для каждого элемента.
        Возвращает tuple (response_text, prompt) или (None, None) при ошибке.
        """
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

            if response.status_code == 200:
                result = response.json()

                if 'choices' in result and len(result['choices']) > 0:
                    analysis_text = result['choices'][0]['message']['content']

                    # Парсим JSON ответ
                    try:
                        # Извлекаем JSON из ответа (на случай если есть лишний текст)
                        json_start = analysis_text.find('{')
                        json_end = analysis_text.rfind('}') + 1
                        if json_start != -1 and json_end > json_start:
                            json_text = analysis_text[json_start:json_end]
                            data = json.loads(json_text)
                        else:
                            data = json.loads(analysis_text)

                        genres_yes = []
                        genres_maybe = []
                        themes_yes = []
                        themes_maybe = []

                        # Обрабатываем жанры
                        for genre in data.get('genres', []):
                            name = genre.get('name', '')
                            decision = genre.get('decision', '').upper()
                            if decision == 'YES':
                                genres_yes.append(name)
                            elif decision == 'MAYBE':
                                genres_maybe.append(name)

                        # Обрабатываем темы
                        for theme in data.get('themes', []):
                            name = theme.get('name', '')
                            decision = theme.get('decision', '').upper()
                            if decision == 'YES':
                                themes_yes.append(name)
                            elif decision == 'MAYBE':
                                themes_maybe.append(name)

                        # Формируем читаемый вывод с суммари
                        readable_output = []
                        readable_output.append("GENRES:\n")
                        for genre in data.get('genres', []):
                            readable_output.append(f"**{genre['name']}**")
                            readable_output.append(f"**YES/NO**: {genre['decision']}")
                            readable_output.append(f"**Explanation**: {genre['explanation']}\n")

                        readable_output.append("\nTHEMES:\n")
                        for theme in data.get('themes', []):
                            readable_output.append(f"**{theme['name']}**")
                            readable_output.append(f"**YES/NO**: {theme['decision']}")
                            readable_output.append(f"**Explanation**: {theme['explanation']}\n")

                        summary = f"\n\n=== SUMMARY ===\n\nMATCHING GENRES:\n"
                        if genres_yes:
                            for genre in sorted(genres_yes):
                                summary += f"- {genre}\n"
                        else:
                            summary += f"- None\n"

                        if genres_maybe:
                            summary += f"\nMAYBE GENRES:\n"
                            for genre in sorted(genres_maybe):
                                summary += f"- {genre}\n"
                        else:
                            summary += f"\nMAYBE GENRES:\n- None\n"

                        summary += f"\nMATCHING THEMES:\n"
                        if themes_yes:
                            for theme in sorted(themes_yes):
                                summary += f"- {theme}\n"
                        else:
                            summary += f"- None\n"

                        if themes_maybe:
                            summary += f"\nMAYBE THEMES:\n"
                            for theme in sorted(themes_maybe):
                                summary += f"- {theme}\n"
                        else:
                            summary += f"\nMAYBE THEMES:\n- None\n"

                        final_output = '\n'.join(readable_output) + summary

                        return final_output, prompt

                    except json.JSONDecodeError as e:
                        # Если JSON не распарсился, возвращаем raw текст
                        return analysis_text, prompt
                else:
                    return None, prompt
            else:
                if response.status_code == 401:
                    self.access_token = None
                return None, None

        except Exception as e:
            return None, None

    def build_game_context(self, game: Game) -> str:
        context_parts = []
        total_chars = 0
        max_chars = 1500

        name_text = f"[GAME] {game.name}"
        context_parts.append(name_text)
        total_chars += len(name_text)

        if game.summary and total_chars < max_chars:
            summary_text = f"[SUMMARY] {game.summary[:300]}"
            if total_chars + len(summary_text) <= max_chars:
                context_parts.append(summary_text)
                total_chars += len(summary_text)

        if game.storyline and total_chars < max_chars:
            remaining = max_chars - total_chars
            storyline_text = f"[STORYLINE] {game.storyline[:min(300, remaining)]}"
            context_parts.append(storyline_text)

        return '\n'.join(context_parts)

    def generate_description(self, game: Game) -> tuple[Optional[str], Optional[str]]:
        """
        Генерирует описание геймплея для игры.
        Возвращает tuple (description, prompt) или (None, None) при ошибке.
        """
        try:
            context = self.build_game_context(game)
            prompt = self.build_prompt(game.name, context)

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
                    return description, prompt
                else:
                    return None, prompt
            else:
                if response.status_code == 401:
                    self.access_token = None
                return None, None

        except Exception as e:
            return None, None

    def build_prompt(self, game_name: str, context: str) -> str:
        return f"""Write a gameplay description for "{game_name}".

{context}

Write 3-4 paragraphs about gameplay mechanics. No markdown."""

    def clean_description(self, description: str) -> str:
        import re

        description = re.sub(r'^Gameplay description:?\s*', '', description, flags=re.IGNORECASE)
        description = re.sub(r'\n\s*\n\s*\n', '\n\n', description)

        return description.strip()

    def show_summary(self) -> None:
        elapsed_time = time.time() - self.start_time

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'✅ Успешно: {self.games_processed}')
        self.stdout.write(f'❌ Не удалось: {self.games_failed}')
        self.stdout.write(f'⏱️  Время: {elapsed_time:.2f} секунд')
        self.stdout.write('=' * 60)