FROM python:3.11-slim

# Устанавливаем Tailscale
RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://tailscale.com/install.sh | sh

# Устанавливаем зависимости Python
# Сначала копируем только requirements.txt (оптимизация)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Скрипт запуска
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]