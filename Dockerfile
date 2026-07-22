FROM python:3.12-slim AS builder

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip wheel --no-cache-dir --wheel-dir /wheels ".[server]"

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN useradd --create-home --shell /usr/sbin/nologin broker
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
COPY examples/contracts.yaml ./contracts.yaml

USER broker
EXPOSE 8000
# The API only accepts and reads effects; run `effect-broker worker` and
# `effect-broker reconciler` as separate processes/containers to dispatch.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).read()"

CMD ["python", "-c", "import uvicorn; from effect_broker.config import Settings, build_broker; from effect_broker.api import create_app; uvicorn.run(create_app(build_broker(Settings())), host='0.0.0.0', port=8000)"]
