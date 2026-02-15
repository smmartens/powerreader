FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid appuser --create-home appuser \
    && mkdir -p /data && chown appuser:appuser /data

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY powerreader/ powerreader/

USER appuser
EXPOSE 8080
VOLUME /data

CMD ["uv", "run", "uvicorn", "powerreader.main:app", "--host", "0.0.0.0", "--port", "8080"]
