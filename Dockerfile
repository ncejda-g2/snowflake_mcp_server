FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml ./
COPY server/ ./server/

RUN pip install --no-cache-dir .

ENTRYPOINT ["snowflake-readonly-mcp"]
