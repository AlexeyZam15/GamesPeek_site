# games/management/commands/import_rawg/base_command.py
import time
import json
import os
from pathlib import Path
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db.models import Q
from games.models import Game


class ImportRawgBaseCommand(BaseCommand):
    """Базовый класс для импорта RAWG описаний"""

    def __init__(self):
        super().__init__()
        self.rawg_client = None
        self.stats_db = None
        self.api_stats = defaultdict(int)
        self.not_found_ids = set()
        self.shutdown_flag = False
        self.balance_exceeded = False

    def add_arguments(self, parser):
        """Добавление аргументов команды"""
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только проверка без сохранения'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Количество игр для обработки (0 = все игры)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=4,
            help='Потоков (рекомендуется 4 для кэширования)'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=0.1,
            help='Задержка между запросами (секунды)'
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Перезаписать существующие описания'
        )
        parser.add_argument(
            '--api-key',
            type=str,
            help='RAWG API ключ (если не указан, берется из .env)'
        )
        parser.add_argument(
            '--min-length',
            type=int,
            default=1,
            help='Минимальная длина описания'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Подробный вывод отладки'
        )
        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить первые N игр'
        )
        parser.add_argument(
            '--order-by',
            type=str,
            default='id',
            choices=['id', 'name', '-rating', '-rating_count', '-first_release_date'],
            help='Поле для сортировки игр'
        )
        parser.add_argument(
            '--log-dir',
            type=str,
            default='logs',
            help='Директория для логов'
        )
        parser.add_argument(
            '--game-ids',
            type=str,
            help='ID конкретных игр для обработки (через запятую)'
        )
        parser.add_argument(
            '--repeat',
            type=int,
            default=0,
            help='Повторить команду N раз (0 = пока не обработаны все игры)'
        )
        parser.add_argument(
            '--repeat-delay',
            type=float,
            default=10.0,
            help='Пауза между повторами в секундах'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=50,
            help='Размер батча за один повтор (по умолчанию: 50)'
        )
        parser.add_argument(
            '--auto-offset',
            action='store_true',
            default=True,
            help='Автоматически пропускать не найденные игры'
        )
        parser.add_argument(
            '--no-auto-offset',
            action='store_false',
            dest='auto_offset',
            help='Выключить автоматический пропуск не найденных игр'
        )
        parser.add_argument(
            '--auto-offset-file',
            type=str,
            default='auto_offset_log.json',
            help='Файл для хранения списка не найденных игр'
        )
        parser.add_argument(
            '--cache-ttl',
            type=int,
            default=30,
            help='Время жизни кэша в днях (0 = бесконечно)'
        )
        parser.add_argument(
            '--skip-cache',
            action='store_true',
            help='Пропустить кэш (для тестирования)'
        )
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Удалить кэш и список ненайденных игр перед началом'
        )
        parser.add_argument(
            '--include-all-gametypes',
            action='store_true',
            help='Включить все типы игр (не только основные)'
        )
        parser.add_argument(
            '--save-on-interrupt',
            action='store_true',
            default=True,
            help='Сохранять прогресс при прерывании'
        )
        parser.add_argument(
            '--no-save-on-interrupt',
            action='store_false',
            dest='save_on_interrupt',
            help='Выключить сохранение прогресса при прерывании'
        )
        parser.add_argument(
            '--ignore-auto-offset',
            action='store_true',
            help='Игнорировать auto-offset для конкретных игр (принудительная обработка)'
        )

    def init_import_settings(self, options):
        """Инициализирует настройки для импорта"""
        self.dry_run = options['dry_run']
        self.limit = options['limit']
        self.workers = options['workers']
        self.delay = options['delay']
        self.overwrite = options['overwrite']
        self.min_length = options['min_length']
        self.debug = options['debug']
        self.offset = options['offset']
        self.order_by = options['order_by']
        self.log_dir = options['log_dir']
        self.skip_cache = options['skip_cache']
        self.include_all_gametypes = options.get('include_all_gametypes', False)