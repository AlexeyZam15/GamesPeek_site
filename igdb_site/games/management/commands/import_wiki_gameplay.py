#!/usr/bin/env python
"""
Главный файл команды импорта Wikipedia описаний
Использует модульную структуру для лучшей организации кода
"""

import os
import sys

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Импортируем команду из папки
from import_wiki_gameplay_folder.command import Command

if __name__ == "__main__":
    # Для прямого запуска (опционально)
    import django

    django.setup()
    command = Command()
    command.execute()