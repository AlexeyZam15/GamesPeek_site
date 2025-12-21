import signal
import sys
import threading
import time


class SignalHandler:
    """Обработчик сигналов для мгновенного завершения"""

    def __init__(self, command):
        self.command = command
        self.shutdown_flag = threading.Event()
        self.interrupted = False

        # Устанавливаем обработчики сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обработчик сигналов - мгновенное завершение"""
        if not self.interrupted:
            self.interrupted = True
            self.shutdown_flag.set()

            # Немедленно выводим сообщение
            if hasattr(self.command, 'stdout'):
                self.command.stdout.write("\n\n🚨 ПОЛУЧЕНО ПРЕРЫВАНИЕ! Завершаю работу...\n")
                self.command.stdout.flush()

            # Даем небольшую паузу для вывода сообщения
            time.sleep(0.1)

            # Немедленный выход
            sys.exit(130)  # Код выхода для Ctrl+C

    def is_shutdown(self):
        """Проверяет, было ли запрошено завершение"""
        return self.shutdown_flag.is_set()

    def wait(self, timeout=None):
        """Ждет завершения или таймаута"""
        return self.shutdown_flag.wait(timeout)