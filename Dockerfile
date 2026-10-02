# Python 3.11 is required: Ray Client refuses to connect when the client's
# Python minor version differs from the cluster's (rayproject/ray:2.40.0-py311).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv/payanam

# librdkafka is required by confluent-kafka; curl is used by the healthcheck.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential librdkafka-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
