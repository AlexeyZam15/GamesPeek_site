"""
Команда Django для генерации описаний жанров и тем через GigaChat (один запрос).
"""

import time
import requests
import uuid
import urllib3
from django.core.management.base import BaseCommand
from django.conf import settings
from games.models import Genre, Theme

# Отключаем SSL предупреждения
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Command(BaseCommand):
    help = 'Генерирует описания жанров и тем одним запросом'

    def add_arguments(self, parser):
        parser.add_argument('--type', choices=['genres', 'themes', 'both'], default='both')
        parser.add_argument('--output-file', default='descriptions.txt')
        parser.add_argument('--dry-run', action='store_true')

    def handle(self, *args, **options):
        self.auth_key = getattr(settings, 'GIGACHAT_AUTH_KEY', None)
        if not self.auth_key:
            self.stdout.write(self.style.ERROR('GIGACHAT_AUTH_KEY не задан'))
            return

        if not self.get_token():
            return

        # Собираем названия
        genres = list(Genre.objects.values_list('name', flat=True)) if options['type'] in ['genres', 'both'] else []
        themes = list(Theme.objects.values_list('name', flat=True)) if options['type'] in ['themes', 'both'] else []

        if options['dry_run']:
            self.stdout.write(f"Жанры ({len(genres)}): {', '.join(genres[:10])}")
            self.stdout.write(f"Темы ({len(themes)}): {', '.join(themes[:10])}")
            return

        # Максимальное количество элементов в одном запросе для безопасной работы с токенами
        MAX_ITEMS_PER_REQUEST = 25

        all_results = {
            "genres": [],
            "themes": []
        }

        # Обрабатываем жанры
        if genres:
            self.stdout.write(f"\n📊 Обработка жанров ({len(genres)} шт.)")
            for i in range(0, len(genres), MAX_ITEMS_PER_REQUEST):
                chunk = genres[i:i + MAX_ITEMS_PER_REQUEST]
                chunk_number = i // MAX_ITEMS_PER_REQUEST + 1
                total_chunks = (len(genres) + MAX_ITEMS_PER_REQUEST - 1) // MAX_ITEMS_PER_REQUEST

                self.stdout.write(f"  Часть {chunk_number}/{total_chunks}...")

                # Формируем список элементов для запроса
                item_list = "\n".join(chunk)

                # Формируем промпт с JSON форматом для жанров
                prompt = f"""You are a video game expert. Describe each genre item below.

CRITICAL: Return ONLY valid JSON format with this exact structure:
[
  {{
    "name": "genre name",
    "definition": "2-3 sentences about what defines this genre",
    "key_features": "comma-separated list of key features",
    "examples": "comma-separated list of game titles"
  }}
]

RULES:
- Return a JSON array of objects
- Each object must have exactly these 4 fields: name, definition, key_features, examples
- Do NOT add any text before or after the JSON
- Do NOT use markdown formatting
- Keep descriptions concise

Genres to describe:
{item_list}
"""

                # Отправляем запрос
                response = self.ask_gigachat(prompt)

                if response:
                    try:
                        import json
                        # Очищаем ответ от возможных маркдаун блоков
                        clean_response = response.strip()
                        if clean_response.startswith('```json'):
                            clean_response = clean_response[7:]
                        if clean_response.startswith('```'):
                            clean_response = clean_response[3:]
                        if clean_response.endswith('```'):
                            clean_response = clean_response[:-3]
                        clean_response = clean_response.strip()

                        data = json.loads(clean_response)

                        # Заменяем Squad Management если он в жанрах
                        for item in data:
                            if item['name'].lower() == 'squad management':
                                item[
                                    'definition'] = "Games where players lead a squad of individualized characters, managing their equipment, skills, and loadouts to suit different missions and strategies."
                                item[
                                    'key_features'] = "Unit customization, equipment management, skill selection, tactical positioning, unit progression"
                                item['examples'] = "*XCOM 2*, *Fire Emblem: Three Houses*, *Valkyria Chronicles*"

                        all_results["genres"].extend(data)

                    except json.JSONDecodeError as e:
                        self.stdout.write(self.style.ERROR(f"  Ошибка парсинга JSON: {e}"))
                        self.stdout.write(f"  Ответ: {response[:200]}")

                # Небольшая пауза между запросами
                if i + MAX_ITEMS_PER_REQUEST < len(genres):
                    self.stdout.write("  Ожидание 2 секунды...")
                    time.sleep(2)

        # Обрабатываем темы
        if themes:
            self.stdout.write(f"\n📊 Обработка тем ({len(themes)} шт.)")
            for i in range(0, len(themes), MAX_ITEMS_PER_REQUEST):
                chunk = themes[i:i + MAX_ITEMS_PER_REQUEST]
                chunk_number = i // MAX_ITEMS_PER_REQUEST + 1
                total_chunks = (len(themes) + MAX_ITEMS_PER_REQUEST - 1) // MAX_ITEMS_PER_REQUEST

                self.stdout.write(f"  Часть {chunk_number}/{total_chunks}...")

                # Формируем список элементов для запроса
                item_list = "\n".join(chunk)

                # Формируем промпт с JSON форматом для тем
                prompt = f"""You are a video game expert. Describe each theme item below.

CRITICAL: Return ONLY valid JSON format with this exact structure:
[
  {{
    "name": "theme name",
    "definition": "2-3 sentences about what defines this theme",
    "key_features": "comma-separated list of key features",
    "examples": "comma-separated list of game titles"
  }}
]

RULES:
- Return a JSON array of objects
- Each object must have exactly these 4 fields: name, definition, key_features, examples
- Do NOT add any text before or after the JSON
- Do NOT use markdown formatting
- Keep descriptions concise

Themes to describe:
{item_list}
"""

                # Отправляем запрос
                response = self.ask_gigachat(prompt)

                if response:
                    try:
                        import json
                        # Очищаем ответ от возможных маркдаун блоков
                        clean_response = response.strip()
                        if clean_response.startswith('```json'):
                            clean_response = clean_response[7:]
                        if clean_response.startswith('```'):
                            clean_response = clean_response[3:]
                        if clean_response.endswith('```'):
                            clean_response = clean_response[:-3]
                        clean_response = clean_response.strip()

                        data = json.loads(clean_response)

                        # Заменяем Squad Management если он в темах
                        for item in data:
                            if item['name'].lower() == 'squad management':
                                item[
                                    'definition'] = "Games where players lead a squad of individualized characters, managing their equipment, skills, and loadouts to suit different missions and strategies."
                                item[
                                    'key_features'] = "Unit customization, equipment management, skill selection, tactical positioning, unit progression"
                                item['examples'] = "*XCOM 2*, *Fire Emblem: Three Houses*, *Valkyria Chronicles*"

                        all_results["themes"].extend(data)

                    except json.JSONDecodeError as e:
                        self.stdout.write(self.style.ERROR(f"  Ошибка парсинга JSON: {e}"))
                        self.stdout.write(f"  Ответ: {response[:200]}")

                # Небольшая пауза между запросами
                if i + MAX_ITEMS_PER_REQUEST < len(themes):
                    self.stdout.write("  Ожидание 2 секунды...")
                    time.sleep(2)

        # Сохраняем результаты в JSON файл
        if all_results["genres"] or all_results["themes"]:
            with open(options['output_file'], 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)
            self.stdout.write(self.style.SUCCESS(f'\n✓ Сохранено в {options["output_file"]}'))
            self.stdout.write(f'  Жанров: {len(all_results["genres"])}')
            self.stdout.write(f'  Тем: {len(all_results["themes"])}')
        else:
            self.stdout.write(self.style.ERROR('Не удалось получить ни одного описания'))

    def clean_description(self, text):
        """
        Очищает описание от маркдауна, звездочек и лишних отступов.
        Приводит формат к единообразному виду.
        """
        lines = text.split('\n')
        cleaned_lines = []

        for line in lines:
            # Удаляем звездочки в начале строки (маркдаун списки)
            if line.lstrip().startswith('* '):
                line = line.replace('* ', '', 1)
            # Удаляем звездочки вокруг текста (жирный/курсив)
            line = line.replace('**', '')
            line = line.replace('*', '')
            # Удаляем лишние пробелы в начале
            line = line.lstrip()
            # Убираем множественные пробелы
            while '  ' in line:
                line = line.replace('  ', ' ')
            cleaned_lines.append(line)

        # Объединяем обратно
        result = '\n'.join(cleaned_lines)

        # Удаляем пустые строки в начале и конце
        result = result.strip()

        return result

    def get_token(self):
        try:
            r = requests.post(
                'https://ngw.devices.sberbank.ru:9443/api/v2/oauth',
                headers={'Authorization': f'Basic {self.auth_key}', 'RqUID': str(uuid.uuid4())},
                data={'scope': 'GIGACHAT_API_PERS'},
                timeout=30, verify=False
            )
            if r.status_code == 200:
                self.token = r.json()['access_token']
                return True
            return False
        except Exception as e:
            self.stdout.write(self.style.ERROR(str(e)))
            return False

    def ask_gigachat(self, prompt):
        try:
            r = requests.post(
                'https://gigachat.devices.sberbank.ru/api/v1/chat/completions',
                headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'},
                json={
                    'model': 'GigaChat',
                    'messages': [{'role': 'user', 'content': prompt}],
                    'temperature': 0.5,
                    'max_tokens': 4000
                },
                timeout=120, verify=False
            )
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
            self.stdout.write(self.style.ERROR(f'Ошибка: {r.status_code}'))
            return None
        except Exception as e:
            self.stdout.write(self.style.ERROR(str(e)))
            return None