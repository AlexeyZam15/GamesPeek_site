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

        # Формируем промпт
        prompt = "Describe each video game genre and theme below. Use this format:\n\n"

        if genres:
            prompt += "GENRES:\n" + "\n".join([f"--- {g} ---" for g in genres]) + "\n\n"

        if themes:
            prompt += "THEMES:\n" + "\n".join([f"--- {t} ---" for t in themes]) + "\n\n"

        prompt += "For each, write 2-3 sentences about what defines it, key features, and examples."

        # Один запрос
        self.stdout.write("Отправка запроса...")
        description = self.ask_gigachat(prompt)

        if description:
            with open(options['output_file'], 'w', encoding='utf-8') as f:
                f.write(description)
            self.stdout.write(self.style.SUCCESS(f'✓ Сохранено в {options["output_file"]}'))

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