FROM python:3.12.13-slim

WORKDIR /app

COPY services/processor_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/common ./services/common
COPY services/processor_worker ./services/processor_worker
COPY services/__init__.py ./services/__init__.py

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.processor_worker.worker"]
