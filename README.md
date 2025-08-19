<div align="center">

[![ArgoCD Badge - G2 MCP server Development](https://acdbadge.tools.g2.com/api/badge?name=g2-mcp-server-development&revision=true&showAppName=true&cache=false)](https://argocd.tools.g2.com/applications/g2-mcp-server-development?resource=)
[![ArgoCD Badge - G2 MCP server Production](https://acdbadge.tools.g2.com/api/badge?name=g2-mcp-server-production&revision=true&showAppName=true&cache=false)](https://argocd.tools.g2.com/applications/g2-mcp-server-production?resource=)

</div>

# G2 MCP Server

A Model Context Protocol (MCP) server that provides access to G2's comprehensive software reviews, ratings, and product data through OAuth authentication and custom manual tools.

## Overview

G2 is the world's largest and most trusted software marketplace. This MCP server allows you to access G2's extensive database of software products, user reviews, categories, and ratings to make informed software purchasing decisions.

## Features

This server provides access to G2's key API endpoints through:

- **Manual Tools**: Custom-built tools optimized for specific G2 API functionality
- **Auto-Discovery**: Automatic tool registration system for easy extensibility

## Prerequisites

- [Mise](https://mise.jdx.dev/) - Runtime version manager
- Python 3.12+ (managed by Mise)
- UV package manager (installed by Mise)


## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/g2crowd/g2-mcp-server.git
   cd g2-mcp-server
   ```

2. Set up the development environment:

   ```bash
   # Trust and install development tools via Mise
   mise trust
   mise install

   # Install Python dependencies
   uv sync

   # Set up pre-commit hooks for code quality
   pre-commit install
   ```

3. The server is now ready to handle OAuth authentication automatically

## Running the Server

Start the MCP server:

```bash
# Using the main entry point
MCP_TRANSPORT=sse MCP_HOST=127.0.0.1 python main.py

# Using UV project script (recommended)
MCP_TRANSPORT=sse MCP_HOST=127.0.0.1 uv run server
```

## Development Commands

```bash
# Install dependencies (including dev tools)
uv sync --extra dev

# Code quality checks
uv run ruff check          # Linting
uv run ruff format         # Code formatting
uv run mypy server         # Type checking

# Testing
uv run pytest             # Run all tests
uv run pytest --cov       # Run tests with coverage
uv run pytest -v          # Verbose test output

# Pre-commit hooks
pre-commit run --all-files # Run all hooks manually
pre-commit install         # Install hooks (first time setup)

# Development server with hot reload
MCP_TRANSPORT=sse MCP_HOST=127.0.0.1 uvicorn server.app:mcp --reload
```

## Configuration

### With Claude Desktop

Add to your Claude Desktop configuration file (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "g2-server": {
      "command": "python",
      "args": ["/path/to/g2-mcp-server/main.py"],
      "env": {
        "G2_API_TOKEN": "<TOKEN>"
      }
    }
  }
}
```

**Note**: Replace <TOKEN> with your G2 API Bearer Token.

## Available Tools

The server provides manually crafted tools organized into focused categories:

### Buyer Intent Tools

- **`browse_buyer_intent_interactions`**: Advanced OLAP-style queries for buyer intent data
  - **Purpose**: Analyze buyer behavior patterns and intent signals for specific products
  - **Key Features**: Dimensional analysis, time series data, comprehensive filtering
  - **Example Usage**:
    ```json
    {
      "subject_product_id": "12345",
      "dimensions": "company_name,day",
      "measures": "intent_score,page_views",
      "dimension_filters": "{\"day_gteq\": \"2024-01-01\", \"company_intent_score_gteq\": \"50\"}"
    }
    ```
  - **Filter Operators**: `_eq`, `_cont`, `_gt`, `_gteq`, `_lt`, `_lteq`, `_present`, `_empty`

### Product Management Tools

- **`list_products`**: List products with advanced filtering options
  - **Purpose**: Search and filter G2's extensive product catalog
  - **Key Filters**: Category, vendor, rating, review count, product slug
  - **Example Usage**:
    ```json
    {
      "filter_category_id": "123",
      "filter_star_rating": 4.0,
      "filter_review_count_gteq": 100,
      "fields": "name,slug,star_rating,review_count",
      "relationships": "categories,vendors"
    }
    ```

- **`list_my_products`**: List products owned by the current account
  - **Purpose**: Access your organization's product listings on G2

- **`show_product`**: Detailed product information with relationship data
  - **Purpose**: Get comprehensive details for a specific product
  - **Includes**: Description, ratings, vendor info, categories, reviews summary

### Category Management Tools

- **`list_categories`**: List categories with filtering and field selection
  - **Purpose**: Browse G2's software category taxonomy
  - **Features**: Custom field selection, relationship inclusion
  - **Example Usage**:
    ```json
    {
      "fields": "name,slug,product_count",
      "relationships": "products,children,parent"
    }
    ```

- **`show_category`**: Detailed category information with hierarchical relationships
  - **Purpose**: Get comprehensive category details including hierarchy
  - **Includes**: Parent/child relationships, associated products, category metadata

### Review Management Tools

- **`list_product_reviews`**: Get product reviews with multiple serializer options
  - **Purpose**: Access detailed user reviews for any G2 product
  - **Features**: Multiple data formats, filtering options, field selection
  - **Example Usage**:
    ```json
    {
      "product_id": "12345",
      "serializer": "market_intelligence",
      "fields": "title,star_rating,comment,submitted_at",
      "relationships": "reviewer"
    }
    ```

- **`show_product_review`**: Get specific review for current user's products
  - **Purpose**: Access detailed review data for your organization's products
  - **Includes**: Full review content, reviewer details, response data

### Vendor Management Tools

- **`list_vendors`**: List vendors with timestamp-based filtering
  - **Purpose**: Browse software vendors in G2's marketplace
  - **Features**: Timestamp filtering, custom field selection
  - **Example Usage**:
    ```json
    {
      "filter_updated_at_gteq": "2024-01-01T00:00:00Z",
      "fields": "name,slug,website,founded_year",
      "relationships": "products"
    }
    ```

- **`show_vendor`**: Detailed vendor information with product relationships
  - **Purpose**: Get comprehensive vendor details and their product portfolio
  - **Includes**: Company info, product listings, vendor statistics

### Common Tool Features

All tools support:
- **Flexible Field Selection**: Choose specific data fields to reduce response size
- **Relationship Inclusion**: Include related resources (products, categories, vendors, etc.)
- **Advanced Filtering**: Multiple filter options with various operators
- **Error Handling**: Comprehensive validation and error reporting
- **Rate Limiting**: Automatic handling of G2 API rate limits

## API Rate Limits

G2 API has the following limits:

- **Rate Limit**: 100 requests per second
- **Throttling**: Blocked for 60 seconds if rate limit exceeded
- **Page Size**: Maximum 100 items per request (varies by endpoint)

## Data Format

All responses follow G2's JSON:API specification and include:

- **Products**: Name, slug, category, review count, average rating, vendor info
- **Reviews**: Star rating, title, comment, reviewer name, submission date
- **Categories**: Name, slug, product count

## Error Handling

The server includes comprehensive error handling for:

- OAuth authentication failures
- Network connectivity issues
- API rate limiting
- Invalid parameters
- Empty result sets

## Environment Variables

| Variable                         | Required | Description                                             |
| -------------------------------- | -------- | ------------------------------------------------------- |
| `G2_BASE_URL`                    | No       | G2 API base URL (default: <https://data.g2.com/api/v2>) |
| `G2_TIMEOUT`                     | No       | Request timeout in seconds (default: 30)                |
| `G2_USER_AGENT`                  | No       | User agent for requests (default: G2-MCP-Server/1.0)    |
| `DEBUG`                          | No       | Enable debug logging (default: true)                    |
| `G2_MAX_PAGE_SIZE`               | No       | Maximum page size for requests (default: 100)           |
| `G2_MCP_URL`                     | No       | OAuth MCP URL (default: <https://mcp-dev.g2.com/mcp>)   |
| `G2_IS_RUNNING_IN_DOCKER`        | No       | Docker environment flag (default: false)                |
| `G2_IS_RUNNING_IN_DOCKER_OR_K8S` | No       | Container environment flag (default: false)             |

**Note**: Authentication uses Bearer Tokens extracted from HTTP headers for API access.

## Troubleshooting

### Common Issues

1. **Bearer token authentication failure**
   - Ensure your Bearer token is valid and not expired
   - Check that the token has appropriate permissions for the requested resources

2. **"Unable to retrieve data"**
   - Check your internet connection
   - Verify Bearer token permissions
   - Check if you've exceeded rate limits

3. **"No data available"**
   - The request was successful but returned no results
   - Try adjusting search parameters or filters

### Debug Mode

Debug logging is enabled by default. To disable it:

```bash
export DEBUG=0
```

## API Documentation

For complete G2 API documentation, visit: <https://data.g2.com/api/docs>

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Security

- Never commit sensitive configuration to version control
- Monitor API usage for unusual activity

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues related to:

- **This MCP Server**: Open an issue in this repository
- **G2 API**: Contact G2 support or check their documentation
- **MCP Protocol**: Visit the [Model Context Protocol documentation](https://modelcontextprotocol.io/)

## Changelog

### v3.0.0

- **Manual Tools Architecture**: Replaced OpenAPI auto-generation with custom manual tools
- **Enhanced Business Intelligence**: Added OLAP-style buyer intent analysis tool
- **Tool Auto-Discovery**: Implemented automatic tool registration system
- **Improved Logging**: Added comprehensive request/response logging utilities
- **Focused Tool Categories**: Organized tools into BI, Product, Category, Review, and Vendor modules
- **Advanced Filtering**: Enhanced filtering capabilities with dimensional analysis
- **Better Documentation**: Improved tool documentation with detailed parameter descriptions

### v2.0.0

- Use API Tokens for Client authentication
- OpenAPI-based tool generation
- Simplified architecture with FastMCP integration
- Automatic endpoint discovery from G2 API specification
