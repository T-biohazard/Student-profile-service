FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends     build-essential libpq-dev curl  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app
COPY tools /app/tools

ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "app.ingest_api:app", "--host", "0.0.0.0", "--port", "8090"]
