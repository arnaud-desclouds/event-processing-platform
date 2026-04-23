FROM python:3.12.13-slim

WORKDIR /workspace

COPY alembic/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY alembic ./alembic
COPY alembic.ini ./alembic.ini

CMD ["alembic", "upgrade", "head"]
