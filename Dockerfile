FROM cgr.dev/chainguard/wolfi-base@sha256:e624c5d5e42382ce7165ddafcbbf8e6769a24cbd02ea6114b880b05ae5ba2a8d AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apk add --no-cache python-3.12 py3.12-pip ca-certificates

WORKDIR /app

COPY pyproject.toml README.md ./
RUN mkdir -p src/mobility_flow \
    && touch src/mobility_flow/__init__.py \
    && pip install --upgrade pip \
    && pip install .
COPY src ./src
RUN pip install --no-deps --force-reinstall .

RUN adduser -D -u 10001 appuser
USER appuser

EXPOSE 8000 9101
CMD ["uvicorn", "mobility_flow.api:app", "--host", "0.0.0.0", "--port", "8000"]
