# Multi-stage build for Snowflake MCP Server
FROM python:3.12-slim AS builder

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files first (for better caching)
COPY pyproject.toml uv.lock* ./

# Create virtual environment and install dependencies
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY . .

# Install the project
RUN uv sync --frozen --no-dev

# Production stage
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY --from=builder /app /app

# Set PATH to use virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Required environment variables (set these when running the container)
ENV SNOWFLAKE_ACCOUNT=""
ENV SNOWFLAKE_USERNAME=""
ENV SNOWFLAKE_WAREHOUSE=""

# Authentication options (choose one):
# 1. Username/Password (simplest for containers):
#    ENV SNOWFLAKE_AUTHENTICATOR="snowflake"
#    ENV SNOWFLAKE_PASSWORD="your-password"
#
# 2. Key Pair (most secure, requires admin setup):
#    ENV SNOWFLAKE_AUTHENTICATOR="snowflake_jwt"
#    ENV SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/key.pem"
#
# 3. External Browser (default, requires GUI):
#    ENV SNOWFLAKE_AUTHENTICATOR="externalbrowser"

# Optional environment variables with defaults
ENV TRANSPORT="stdio"
ENV HOST="127.0.0.1"
ENV PORT="8000"
ENV DEBUG="false"

# Run the MCP server
ENTRYPOINT ["python", "-m", "server.app"]
