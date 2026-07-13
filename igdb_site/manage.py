#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""

import os
import sys
import codecs


def main():
    """Run administrative tasks."""
    # Устанавливаем кодировку UTF-8 для stdout и stderr
    # Это позволяет корректно отображать эмодзи и Unicode символы в Windows консоли
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()