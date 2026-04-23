FROM python:3.12.13-slim

WORKDIR /app

COPY services/ingestion_api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/common ./services/common
COPY services/ingestion_api ./services/ingestion_api
COPY services/__init__.py ./services/__init__.py

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "services.ingestion_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
