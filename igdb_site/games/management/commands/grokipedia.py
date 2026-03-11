from django.core.management.base import BaseCommand, CommandError
from grokipedia import from_url, search, Grokipedia
from grokipedia.errors import (
    GrokipediaError,
    HttpStatusError,
    PageNotFoundError,
    RobotsDisallowedError,
    RobotsUnavailableError,
    FetchError,
    ParseError
)
import json
import time
import os
from datetime import datetime


class Command(BaseCommand):
    help = 'Получает описание с Grokipedia.com через библиотеку grokipedia-py'

    def add_arguments(self, parser):
        parser.add_argument('query', type=str, help='Название статьи или поисковый запрос')
        parser.add_argument('--search', action='store_true', help='Выполнить поиск')
        parser.add_argument('--full', action='store_true', help='Показать полный текст (Markdown)')
        parser.add_argument('--json', action='store_true', help='Вывод в формате JSON')
        parser.add_argument('--limit', type=int, default=10, help='Лимит результатов поиска')
        parser.add_argument('--no-respect-robots', action='store_true',
                            help='Игнорировать robots.txt (если сайт блокирует)')
        parser.add_argument('--debug', action='store_true', help='Режим отладки')
        parser.add_argument('--export', action='store_true', help='Экспортировать в файл')
        parser.add_argument('--export-dir', type=str, default='grokipedia_exports',
                            help='Директория для экспорта (по умолчанию: grokipedia_exports)')

    def handle(self, *args, **options):
        self.query = options['query']
        self.is_search = options['search']
        self.full_text = options['full']
        self.as_json = options['json']
        self.limit = options['limit']
        self.respect_robots = not options['no_respect_robots']
        self.debug = options['debug']
        self.export = options['export']
        self.export_dir = options['export_dir']

        try:
            if self.is_search:
                results = self.handle_search()
                if self.export and results:
                    self.export_search_results(results)
            else:
                page_data = self.handle_page()
                if self.export and page_data:
                    self.export_page(page_data)

        except PageNotFoundError as e:
            raise CommandError(f"❌ Страница не найдена: {e}")
        except RobotsDisallowedError as e:
            self.stdout.write(
                self.style.WARNING(
                    f"\n❌ Доступ запрещен robots.txt для {e}\n"
                    "💡 Попробуйте с флагом: --no-respect-robots\n"
                    "   Пример: python manage.py grokipedia \"Tear Ring Saga\" --no-respect-robots"
                )
            )
        except RobotsUnavailableError as e:
            self.stdout.write(
                self.style.WARNING(
                    f"\n❌ Не удалось загрузить robots.txt: {e}\n"
                    "💡 Попробуйте с флагом: --no-respect-robots"
                )
            )
        except HttpStatusError as e:
            raise CommandError(f"❌ Ошибка HTTP {e.status_code} для {e.url}")
        except FetchError as e:
            raise CommandError(f"❌ Ошибка загрузки: {e}")
        except ParseError as e:
            raise CommandError(f"❌ Ошибка парсинга: {e}")
        except GrokipediaError as e:
            raise CommandError(f"❌ Ошибка Grokipedia: {e}")
        except Exception as e:
            if self.debug:
                import traceback
                traceback.print_exc()
            raise CommandError(f"❌ Неожиданная ошибка: {e}")

    def handle_search(self):
        """Обработка поиска"""
        self.stdout.write(f"🔍 Поиск: '{self.query}'")

        try:
            results = search(self.query, limit=self.limit, respect_robots=self.respect_robots)
        except Exception as e:
            if self.debug:
                self.stdout.write(self.style.WARNING(f"   Поиск не удался: {e}"))

            wiki = Grokipedia(respect_robots=self.respect_robots, verbose=self.debug)
            results = wiki.search(self.query)

        if self.as_json:
            self.stdout.write(json.dumps(results, ensure_ascii=False, indent=2))
            return results

        if not results:
            self.stdout.write(self.style.WARNING("❌ Ничего не найдено"))
            return results

        self.stdout.write(self.style.SUCCESS(f"\n✅ Найдено результатов: {len(results)}\n"))

        for idx, url in enumerate(results, 1):
            page_name = url.split('/')[-1].replace('_', ' ')
            self.stdout.write(f"{idx}. {page_name}")
            self.stdout.write(f"   🔗 {url}")

            if idx == 1 and not self.as_json:
                try:
                    time.sleep(0.5)
                    page = from_url(url, respect_robots=self.respect_robots)
                    if page.lede_text:
                        intro = page.lede_text[:200] + "..." if len(page.lede_text) > 200 else page.lede_text
                        self.stdout.write(f"   📝 {intro}")
                except Exception as e:
                    if self.debug:
                        self.stdout.write(self.style.WARNING(f"   Не удалось получить превью: {e}"))

            self.stdout.write("")

        return results

    def handle_page(self):
        """Обработка получения страницы"""

        if self.query.startswith('http'):
            url = self.query
            page_name = url.split('/')[-1]
        else:
            page_name = self.query.strip().replace(' ', '_')
            url = f"https://grokipedia.com/page/{page_name}"

        self.stdout.write(f"🌐 Загрузка: {url}")

        page = from_url(url, respect_robots=self.respect_robots)

        if self.as_json:
            self.stdout.write(page.to_json(indent=2))
            return page

        # Вывод в консоль
        self.print_page_to_console(page)

        return page

    def print_page_to_console(self, page):
        """Вывод страницы в консоль"""
        self.stdout.write(self.style.SUCCESS(f"\n{'=' * 80}"))
        self.stdout.write(f"📄 {page.title}")
        self.stdout.write(f"🔗 {page.url}")
        self.stdout.write(f"{'=' * 80}\n")

        if page.lede_text:
            self.stdout.write(page.lede_text)
            self.stdout.write("")

        if self.full_text:
            self.stdout.write(self.style.SUCCESS("\n📝 ПОЛНЫЙ ТЕКСТ (Markdown):"))
            self.stdout.write("-" * 40)
            self.stdout.write(page.markdown)

        if page.infobox:
            self.stdout.write(f"\n{self.style.SUCCESS('📊 ИНФОБОКС:')}")
            self.stdout.write("-" * 30)
            for field in page.infobox[:7]:
                self.stdout.write(f"  • {field.label}: {field.value}")

        if page.sections:
            self.stdout.write(f"\n{self.style.SUCCESS('📑 ОСНОВНЫЕ РАЗДЕЛЫ:')}")
            self.stdout.write("-" * 30)
            for section in page.sections[:5]:
                self.stdout.write(f"  • {section.title}")

        if page.links:
            self.stdout.write(f"\n{self.style.SUCCESS('🔗 ПЕРВЫЕ ССЫЛКИ:')}")
            self.stdout.write("-" * 30)
            for link in page.links[:5]:
                self.stdout.write(f"  • {link}")

        if page.metadata and page.metadata.keywords:
            self.stdout.write(f"\n{self.style.SUCCESS('🏷️ КЛЮЧЕВЫЕ СЛОВА:')}")
            self.stdout.write(", ".join(page.metadata.keywords[:10]))

        self.stdout.write(f"\n{self.style.SUCCESS('📊 СТАТИСТИКА:')}")
        self.stdout.write(f"  • Разделов: {len(page.sections) if page.sections else 0}")
        self.stdout.write(f"  • Ссылок: {len(page.links) if page.links else 0}")
        if hasattr(page, 'references') and page.references:
            self.stdout.write(f"  • Рефереренсов: {len(page.references)}")

        self.stdout.write(f"\n{self.style.SUCCESS('=' * 80)}")

    def export_page(self, page):
        """Экспорт страницы в файл"""
        # Создаем директорию для экспорта
        os.makedirs(self.export_dir, exist_ok=True)

        # Генерируем имя файла
        safe_title = "".join(c for c in page.title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_title = safe_title.replace(' ', '_')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{safe_title}_{timestamp}.txt"
        filepath = os.path.join(self.export_dir, filename)

        self.stdout.write(f"💾 Экспорт в файл: {filepath}")

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"{'=' * 80}\n")
            f.write(f"Grokipedia Export\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'=' * 80}\n\n")

            f.write(f"ЗАГОЛОВОК: {page.title}\n")
            f.write(f"URL: {page.url}\n")
            f.write(f"{'=' * 80}\n\n")

            if page.lede_text:
                f.write("ВСТУПЛЕНИЕ:\n")
                f.write("-" * 40 + "\n")
                f.write(page.lede_text)
                f.write("\n\n")

            if self.full_text and page.markdown:
                f.write("ПОЛНЫЙ ТЕКСТ:\n")
                f.write("-" * 40 + "\n")
                f.write(page.markdown)
                f.write("\n\n")

            if page.infobox:
                f.write("ИНФОБОКС:\n")
                f.write("-" * 40 + "\n")
                for field in page.infobox:
                    f.write(f"{field.label}: {field.value}\n")
                f.write("\n")

            if page.sections:
                f.write("РАЗДЕЛЫ:\n")
                f.write("-" * 40 + "\n")
                for i, section in enumerate(page.sections, 1):
                    f.write(f"{i}. {section.title}\n")
                f.write("\n")

            if page.links:
                f.write("ССЫЛКИ:\n")
                f.write("-" * 40 + "\n")
                for i, link in enumerate(page.links[:20], 1):  # Первые 20 ссылок
                    f.write(f"{i}. {link}\n")
                f.write("\n")

            if page.metadata and page.metadata.keywords:
                f.write("КЛЮЧЕВЫЕ СЛОВА:\n")
                f.write("-" * 40 + "\n")
                f.write(", ".join(page.metadata.keywords))
                f.write("\n\n")

            f.write(f"{'=' * 80}\n")
            f.write(f"Статистика:\n")
            f.write(f"  Разделов: {len(page.sections) if page.sections else 0}\n")
            f.write(f"  Ссылок: {len(page.links) if page.links else 0}\n")
            if hasattr(page, 'references') and page.references:
                f.write(f"  Рефереренсов: {len(page.references)}\n")
            f.write(f"{'=' * 80}\n")

        self.stdout.write(self.style.SUCCESS(f"✅ Экспорт завершен: {filepath}"))
        return filepath

    def export_search_results(self, results):
        """Экспорт результатов поиска в файл"""
        os.makedirs(self.export_dir, exist_ok=True)

        safe_query = "".join(c for c in self.query if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_query = safe_query.replace(' ', '_')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"search_{safe_query}_{timestamp}.txt"
        filepath = os.path.join(self.export_dir, filename)

        self.stdout.write(f"💾 Экспорт результатов поиска в: {filepath}")

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"{'=' * 80}\n")
            f.write(f"Grokipedia Search Results\n")
            f.write(f"Запрос: {self.query}\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'=' * 80}\n\n")

            for idx, url in enumerate(results, 1):
                page_name = url.split('/')[-1].replace('_', ' ')
                f.write(f"{idx}. {page_name}\n")
                f.write(f"   URL: {url}\n\n")

        self.stdout.write(self.style.SUCCESS(f"✅ Экспорт завершен: {filepath}"))
        return filepath