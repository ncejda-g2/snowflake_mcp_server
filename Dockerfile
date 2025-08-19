ARG PYTHON_VERSION=3.12-slim

# Base stage for shared setup
FROM python:${PYTHON_VERSION} AS base

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files and source code (needed for editable install)
COPY pyproject.toml uv.lock ./
COPY server ./server
COPY main.py ./
COPY newrelic.ini ./

# Production dependencies stage
FROM base AS prod-deps

RUN uv sync --frozen --no-dev

# Build stage for development/testing
FROM base AS build

RUN uv sync --frozen

# Final production stage
FROM python:${PYTHON_VERSION} AS final

WORKDIR /app

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy only production dependencies and app from previous stages
COPY --from=prod-deps /app/.venv /app/.venv
COPY server ./server
COPY main.py ./
COPY newrelic.ini ./

# Set up environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV NEW_RELIC_CONFIG_FILE=/app/newrelic.ini

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["newrelic-admin", "run-program", "python", "main.py"]
