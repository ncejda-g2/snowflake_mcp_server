#!/usr/bin/env python3
"""Test client for Snowflake MCP Server"""

import asyncio
import json

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_mcp_server():
    """Test the MCP server with basic operations"""

    # Connect to the running server
    server_params = StdioServerParameters(command="python", args=["main.py"])

    async with stdio_client(server_params) as (read, write), ClientSession(
        read, write
    ) as session:
            # Initialize the session
            await session.initialize()

            # List available tools
            tools = await session.list_tools()
            print("Available tools:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")

            # Test refresh_catalog
            print("\n1. Testing refresh_catalog...")
            result = await session.call_tool(
                "refresh_catalog", arguments={"force": False}
            )
            print(f"   Result: {json.dumps(result.content, indent=2)}")

            # Test inspect_schemas
            print("\n2. Testing inspect_schemas...")
            result = await session.call_tool("inspect_schemas", arguments={})
            print(
                f"   Result: {json.dumps(result.content, indent=2)[:500]}..."
            )  # Show first 500 chars

            # Test search_tables
            print("\n3. Testing search_tables...")
            result = await session.call_tool(
                "search_tables", arguments={"search_term": "customer"}
            )
            print(f"   Result: {json.dumps(result.content, indent=2)[:500]}...")

            # Test a simple query
            print("\n4. Testing execute_query...")
            result = await session.call_tool(
                "execute_query", arguments={"query": "SHOW DATABASES"}
            )
            print(f"   Result: {json.dumps(result.content, indent=2)[:500]}...")

            print("\nAll tests completed successfully!")


if __name__ == "__main__":
    asyncio.run(test_mcp_server())
