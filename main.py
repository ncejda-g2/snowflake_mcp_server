#!/usr/bin/env python3
"""Main entry point for Snowflake MCP Server."""

import sys
import os

from server.app import mcp, config


def main():
    """Main entry point."""
    # Validate required environment variables
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USERNAME", 
        "SNOWFLAKE_WAREHOUSE"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("Error: Missing required environment variables:", file=sys.stderr)
        for var in missing_vars:
            print(f"  - {var}", file=sys.stderr)
        print("\nPlease set these environment variables before starting the server.", file=sys.stderr)
        print("\nExample:", file=sys.stderr)
        print('  export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"', file=sys.stderr)
        print('  export SNOWFLAKE_USERNAME="user@company.com"', file=sys.stderr)
        print('  export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"', file=sys.stderr)
        sys.exit(1)
    
    # Run the server
    try:
        if config.transport == "stdio":
            mcp.run(transport="stdio")
        elif config.transport == "http":
            mcp.run(transport="http", host=config.host, port=config.port)
        else:
            print(f"Unknown transport: {config.transport}", file=sys.stderr)
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nServer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()