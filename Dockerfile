# Python 3.11 is required: Ray Client refuses to connect when the client's
# Python minor version differs from the cluster's (rayproject/ray:2.40.0-py311).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv/payanam

# curl is used by the healthcheck. Phase 9 dropped build-essential and
# librdkafka-dev along with confluent-kafka: aiokafka is pure Python.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# The chaos harness needs httpx-sse, a dev dependency. Installing both keeps
# `docker compose --profile chaos run` self-contained without a sidecar.
COPY requirements-dev.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements-dev.txt

COPY app ./app
# The chaos harness runs as a compose profile service (see docker-compose.yml).
COPY scripts ./scripts

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
