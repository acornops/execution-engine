FROM python:3.12.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO

WORKDIR /app

RUN apt-get update -qq \
    && apt-get upgrade -y -qq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

FROM base AS production

RUN groupadd --system execution-engine \
    && useradd --system --gid execution-engine --home-dir /app execution-engine

COPY execution_engine ./execution_engine

USER execution-engine

EXPOSE 8080

CMD ["python", "-m", "execution_engine.serve"]

FROM production AS test

USER root
COPY constraints.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -c constraints.txt -r requirements-dev.txt
COPY tests ./tests
COPY scripts ./scripts
COPY docs/contracts ./docs/contracts
USER execution-engine
