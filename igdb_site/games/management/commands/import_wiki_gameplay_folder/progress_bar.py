import sys
import time
import math


class ProgressBar:
    """Улучшенный прогресс-бар с обновлением на месте"""

    def __init__(self, total: int, prefix: str = '📈', length: int = 50,
                 fill: str = '█', empty_fill: str = '░', show_percent: bool = True,
                 show_eta: bool = True, show_speed: bool = True,
                 show_errors: bool = True, show_chunk: bool = False,
                 show_saved: bool = True):
        self.total = total
        self.prefix = prefix
        self.length = length
        self.fill = fill
        self.empty_fill = empty_fill
        self.show_percent = show_percent
        self.show_eta = show_eta
        self.show_speed = show_speed
        self.show_errors = show_errors
        self.show_chunk = show_chunk
        self.show_saved = show_saved

        self.current = 0
        self.errors = 0
        self.not_found = 0  # Счетчик для "не найдено"
        self.saved = 0
        self.start_time = time.time()
        self.last_update = 0
        self.last_count = 0
        self.speed = 0
        self.chunk_info = ""
        self.completed = False

        # Очищаем строку перед началом
        sys.stdout.write('\n')
        self._clear_line()

    def _clear_line(self):
        """Очистить текущую строку в терминале"""
        sys.stdout.write('\r' + ' ' * 150 + '\r')
        sys.stdout.flush()

    def update(self, force: bool = False):
        """Обновить прогресс-бар на месте"""
        now = time.time()

        # Обновляем не чаще чем раз в 0.1 секунды
        if not force and now - self.last_update < 0.1 and not self.completed:
            return

        self.last_update = now

        # Рассчитываем скорость
        if self.current > self.last_count:
            time_diff = now - self.last_update
            if time_diff > 0:
                self.speed = (self.current - self.last_count) / time_diff
            self.last_count = self.current

        # Процент выполнения
        percent = self.current / self.total if self.total > 0 else 0

        # Прогресс-бар
        filled_length = int(self.length * percent)
        bar = self.fill * filled_length + self.empty_fill * (self.length - filled_length)

        # Собираем строку с отступами после эмодзи
        line_parts = [f'{self.prefix} ']
        line_parts.append(f'[{bar}] ')

        if self.show_percent:
            line_parts.append(f'{percent:.1%} ')

        line_parts.append(f'({self.current:,}/{self.total:,}) ')

        if self.show_saved and self.saved > 0:
            line_parts.append(f'💾 {self.saved:,} ')

        if self.show_errors:
            if self.not_found > 0:
                line_parts.append(f'⚪ {self.not_found:,} ')
            if self.errors > 0:
                line_parts.append(f'❌ {self.errors:,} ')

        if self.show_speed and self.speed > 0:
            # Преобразуем скорость в читаемый формат
            if self.speed < 1:
                speed_str = f'{self.speed:.1f}'
            else:
                speed_str = f'{self.speed:.0f}'
            line_parts.append(f'⚡ {speed_str}/с ')

        if self.show_eta and self.current > 0 and percent < 1:
            elapsed = now - self.start_time
            if percent > 0:
                total_time = elapsed / percent
                remaining = total_time - elapsed

                if remaining < 60:
                    eta_str = f'{remaining:.0f}с'
                elif remaining < 3600:
                    minutes = int(remaining // 60)
                    seconds = int(remaining % 60)
                    eta_str = f'{minutes}м{seconds:02d}с'
                else:
                    hours = int(remaining // 3600)
                    minutes = int((remaining % 3600) // 60)
                    eta_str = f'{hours}ч{minutes:02d}м'

                line_parts.append(f'⏱️ {eta_str} ')

        if self.show_chunk and self.chunk_info:
            line_parts.append(f'↻ {self.chunk_info} ')

        # Формируем полную строку
        line = ''.join(line_parts).rstrip()

        # Очищаем и выводим
        self._clear_line()
        sys.stdout.write(line)
        sys.stdout.flush()

    def increment(self, amount: int = 1, is_error: bool = False, is_not_found: bool = False, is_saved: bool = False):
        """Увеличить счетчик"""
        self.current += amount
        if is_error:
            self.errors += 1
        if is_not_found:
            self.not_found += 1
        if is_saved:
            self.saved += amount
        self.update()

    def increment_saved(self, amount: int = 1):
        """Увеличить счетчик сохраненных"""
        self.saved += amount
        self.update()

    def set_chunk_info(self, chunk_info: str):
        """Установить информацию о текущем чанке"""
        self.chunk_info = chunk_info
        self.update()

    def set_progress(self, current: int, errors: int = None, not_found: int = None, saved: int = None):
        """Установить прогресс"""
        self.current = current
        if errors is not None:
            self.errors = errors
        if not_found is not None:
            self.not_found = not_found
        if saved is not None:
            self.saved = saved
        self.update(force=True)

    def complete(self):
        """Завершить прогресс-бар"""
        self.completed = True
        self.current = self.total

        # Финальное обновление с эмодзи завершения
        now = time.time()
        elapsed = now - self.start_time

        # Собираем финальную строку
        line_parts = [f'✅ ']
        line_parts.append(f'[{self.fill * self.length}] ')
        line_parts.append(f'100.0% ')
        line_parts.append(f'({self.total:,}/{self.total:,}) ')

        if self.saved > 0:
            line_parts.append(f'💾 {self.saved:,} ')

        if self.not_found > 0:
            line_parts.append(f'⚪ {self.not_found:,} ')
        if self.errors > 0:
            line_parts.append(f'❌ {self.errors:,} ')

        if elapsed < 60:
            time_str = f'{elapsed:.1f}с'
        elif elapsed < 3600:
            time_str = f'{int(elapsed // 60)}м{int(elapsed % 60):02d}с'
        else:
            time_str = f'{int(elapsed // 3600)}ч{int((elapsed % 3600) // 60):02d}м'

        line_parts.append(f'⏱️ {time_str} ')

        # Финальная скорость
        if elapsed > 0:
            final_speed = self.total / elapsed
            if final_speed < 1:
                speed_str = f'{final_speed:.1f}'
            else:
                speed_str = f'{final_speed:.0f}'
            line_parts.append(f'⚡ {speed_str}/с')

        line = ''.join(line_parts).rstrip()

        self._clear_line()
        sys.stdout.write(line)
        sys.stdout.flush()
        sys.stdout.write('\n')
        sys.stdout.flush()

    def get_elapsed_time(self) -> float:
        """Получить прошедшее время"""
        return time.time() - self.start_time

    def get_speed(self) -> float:
        """Получить текущую скорость"""
        return self.speed

    def get_saved_count(self) -> int:
        """Получить количество сохраненных"""
        return self.saved
