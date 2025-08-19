# G2 MCP Server - Claude Development Guide

This is a Model Context Protocol (MCP) server that provides access to G2's software reviews and ratings data using OAuth authentication and OpenAPI specification.

## Project Structure

- `g2_mcp_server/`
  - `app.py` - MCP server main entry point with OAuth and manual tools integration
  - `config.py` - Configuration management with validation
  - `log_utils.py` - Logging utilities for request/response tracking
  - `tools/` - Manual tool implementations
    - `__init__.py` - Auto-discovery and registration of tools
    - `bi_tools.py` - Business intelligence and buyer intent tools
    - `category_tools.py` - Category management tools
    - `product_tools.py` - Product listing and detail tools
    - `review_tools.py` - Review management tools
    - `vendor_tools.py` - Vendor management tools
  - `__init__.py` - Package initialization
  - `__main__.py` - CLI entry point
- `main.py` - Entry point for running the server
- `pyproject.toml` - Python project configuration and dependencies
- `mise.toml` - Development environment configuration

## Development Setup

```bash
# Install dependencies
mise trust
mise install
uv sync

# Run the server
MCP_HOST=127.0.0.1 python main.py
```

## Key Components

### OAuth Authentication (`g2_mcp_server/app.py`)
- Uses FastMCP OAuth integration with G2's MCP URL
- Automatic token management and refresh
- Base URL: `https://data.g2.com/api/v2`
- OAuth endpoint: `https://mcp-dev.g2.com/mcp`

### Manual Tools Architecture
- Custom tool implementations in `g2_mcp_server/tools/`
- Auto-discovery system automatically loads and registers all tools
- Each tool module contains focused functionality for specific G2 API domains
- Tools are manually crafted for optimal parameter handling and documentation

### Available Tools

#### Business Intelligence Tools (`bi_tools.py`)
- **`browse_buyer_intent_interactions_tool`**: OLAP-style query tool for buyer intent data
  - Supports dimensions, measures, and advanced filtering
  - Time series and aggregated data analysis
  - Comprehensive filter operators (eq, cont, gt, gteq, lt, lteq, etc.)
  - Sorting and dimensional analysis capabilities

#### Category Tools (`category_tools.py`)
- **`list_categories`**: List all categories with filtering and field selection
- **`show_category`**: Retrieve detailed information for a specific category
- Supports relationship inclusion (products, children, ancestors, descendants, parent)

#### Product Tools (`product_tools.py`)
- **`list_products`**: List all products with comprehensive filtering options
- **`list_my_products`**: List products owned by the current account
- **`show_product`**: Retrieve detailed information for a specific product
- Advanced filtering by category, ratings, vendor, and more

#### Review Tools (`review_tools.py`)
- **`list_product_reviews`**: Get reviews for specific products
- **`show_product_review`**: Get specific product review for current user
- Support for both standard and market intelligence serializers
- Flexible field selection and relationship inclusion

#### Vendor Tools (`vendor_tools.py`)
- **`list_vendors`**: List all vendors with timestamp filtering
- **`show_vendor`**: Retrieve detailed information for a specific vendor
- Support for product relationships and comprehensive field selection

### Configuration (`g2_mcp_server/config.py`)
- `Config` - Pydantic model for configuration with validation
- Environment variable support with defaults
- Validation for URLs, timeouts, and other parameters

## Testing

To test the server:
1. Run the server with `MCP_HOST=127.0.0.1 python main.py`
2. Connect via `npx @modelcontextprotocol/inspector`
3. OAuth flow will handle authentication automatically
4. Test with sample queries through the inspector

## Common Commands

```bash
# Install dependencies (including dev tools)
uv sync --extra dev

# Run the server
MCP_HOST=127.0.0.1 python main.py

# Run via project script
uv run server

# Check dependencies
uv tree

# Lint and format
ruff check
ruff format

# Run pre-commit hooks
pre-commit run --all-files

# Install pre-commit hooks (first time)
pre-commit install
```

## Configuration Notes

- Server name: "G2 MCP Server"
- Uses FastMCP framework with OAuth and manual tools integration
- All API requests include proper OAuth authentication
- Request/response logging for debugging via `log_utils.py`
- Automatic redirect following
- Tool auto-discovery system for easy extensibility

## Code Quality

- **Ruff**: Configured for linting and formatting with modern Python standards
- **Pre-commit**: Automated code quality checks on every commit
- **Type Safety**: Modern Python type hints using `|` union syntax

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `G2_BASE_URL` | No | G2 API base URL (default: https://data.g2.com/api/v2) |
| `G2_TIMEOUT` | No | Request timeout in seconds (default: 30) |
| `G2_USER_AGENT` | No | User agent for requests (default: G2-MCP-Server/1.0) |
| `DEBUG` | No | Enable debug logging (default: true) |
| `G2_MAX_PAGE_SIZE` | No | Maximum page size for requests (default: 100) |
| `G2_MCP_URL` | No | OAuth MCP URL (default: https://mcp-dev.g2.com/mcp) |
| `G2_API_TOKEN` | No | API token for G2 API authentication (used in STDIO mode) |
| `G2_IS_RUNNING_IN_DOCKER` | No | Docker environment flag (default: false) |
| `G2_IS_RUNNING_IN_DOCKER_OR_K8S` | No | Container environment flag (default: false) |

**Note**: Authentication is handled via Bearer Token headers for HTTP transports, or via `G2_API_TOKEN` environment variable for STDIO transport.
