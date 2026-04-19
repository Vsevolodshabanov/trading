FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY tests ./tests
COPY README.md pytest.ini .env.example ./

EXPOSE 8000

CMD ["uvicorn", "tbank_trader.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
