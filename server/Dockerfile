# Используем официальный Python образ
FROM python:3.12-slim

# Рабочая директория
WORKDIR /app

# Копируем requirements
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY . .

# По умолчанию запускаем FastAPI
# (для worker'а переопределим command в docker-compose)
CMD ["uvicorn", "app.api.deps:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]