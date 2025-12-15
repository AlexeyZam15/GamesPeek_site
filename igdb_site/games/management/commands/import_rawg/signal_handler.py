# games/management/commands/import_rawg/signal_handler.py
import signal
import sys
import threading


class SignalHandler:
    """Обработчик сигналов для graceful shutdown"""

    def __init__(self, command):
        self.command = command
        self.shutdown_flag = threading.Event()
        self.interrupted = False

        # Регистрируем обработчик сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обработчик сигналов"""
        if not self.interrupted:
            self.interrupted = True
            self.shutdown_flag.set()
            self.command.stdout.write("\n\n⚠️  Получен сигнал прерывания (Ctrl+C)...")
            self.command.stdout.write("🔄 Завершаю работу корректно...")
        else:
            # Второе нажатие Ctrl+C - принудительное завершение
            self.command.stdout.write("\n🚨 Принудительное завершение!")
            sys.exit(1)

    def is_shutdown(self):
        """Проверяет, было ли запрошено завершение"""
        return self.shutdown_flag.is_set()

    def wait(self, timeout=None):
        """Ждет завершения или таймаута"""
        return self.shutdown_flag.wait(timeout)

    def handle_keyboard_interrupt(self, options):
        """Обработка KeyboardInterrupt"""
        self.command.stdout.write("\n\n" + "⚠️" * 20)
        self.command.stdout.write(self.command.style.WARNING("🚨 ПРЕРЫВАНИЕ ВЫПОЛНЕНИЯ КОМАНДЫ"))
        self.command.stdout.write("=" * 50)

        # Показываем статистику если есть
        if hasattr(self.command, 'stats'):
            self.command.stdout.write(f"📊 Текущая статистика:")
            self.command.stdout.write(f"   Обработано игр: {self.command.stats.get('total', 0)}")
            self.command.stdout.write(f"   Найдено описаний: {self.command.stats.get('found', 0)}")
            self.command.stdout.write(f"   Сохранено описаний: {self.command.stats.get('updated', 0)}")
            self.command.stdout.write(f"   Ошибок: {self.command.stats.get('errors', 0)}")

        sys.exit(130)