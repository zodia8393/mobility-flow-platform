FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
RUN mkdir -p src/mobility_flow \
    && touch src/mobility_flow/__init__.py \
    && pip install --upgrade pip \
    && pip install .
COPY src ./src
RUN pip install --no-deps --force-reinstall .

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000 9101
CMD ["uvicorn", "mobility_flow.api:app", "--host", "0.0.0.0", "--port", "8000"]
