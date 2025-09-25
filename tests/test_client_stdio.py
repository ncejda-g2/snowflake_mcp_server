#!/usr/bin/env python3
"""Test client for Snowflake MCP Server using STDIO transport"""

import asyncio
import json
import os

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_mcp_server():
    """Test the MCP server with basic operations"""

    # Load environment variables from .env file
    load_dotenv()

    # Set up environment variables needed by the server
    env = os.environ.copy()

    # Check if environment variables are already set
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USERNAME", "SNOWFLAKE_WAREHOUSE"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        print("❌ Missing required environment variables:")
        for var in missing:
            print(f"   - {var}")
        print("\nPlease add these to your .env file:")
        print("SNOWFLAKE_ACCOUNT=your-account")
        print("SNOWFLAKE_USERNAME=your-username")
        print("SNOWFLAKE_WAREHOUSE=your-warehouse")
        return

    # Create server parameters to spawn a new instance for testing
    server_params = StdioServerParameters(command="python", args=["main.py"], env=env)

    print("🚀 Starting test client for Snowflake MCP Server...")
    print("=" * 60)

    try:
        async with (
            stdio_client(server_params) as (read, write),
            ClientSession(read, write) as session,
        ):
            # Initialize the session
            print("📡 Initializing connection...")
            await session.initialize()
            print("✅ Connected successfully!")

            # List available tools
            print("\n📋 Available tools:")
            print("-" * 40)
            tools = await session.list_tools()
            for tool in tools.tools:
                print(f"  • {tool.name}")
                if tool.description:
                    print(f"    {tool.description[:80]}...")

            # Test 1: Refresh catalog
            print("\n🔄 Test 1: Refreshing catalog...")
            print("-" * 40)
            try:
                result = await session.call_tool(
                    "refresh_catalog", arguments={"force": False}
                )
                content = result.content[0].text if result.content else "No content"
                print(f"✅ Success: {content[:200]}...")
            except Exception as e:
                print(f"❌ Failed: {e}")

            # Test 2: Inspect schemas
            print("\n🔍 Test 2: Inspecting schemas...")
            print("-" * 40)
            try:
                result = await session.call_tool("inspect_schemas", arguments={})
                content = result.content[0].text if result.content else "No content"
                # Parse and display nicely
                try:
                    data = json.loads(content)
                    if "databases" in data:
                        print(f"✅ Found {len(data['databases'])} databases")
                        for db in data["databases"][:3]:  # Show first 3
                            print(f"   - {db}")
                        if len(data["databases"]) > 3:
                            print(f"   ... and {len(data['databases']) - 3} more")
                    else:
                        print(f"✅ Result: {content[:200]}...")
                except Exception:
                    print(f"✅ Result: {content[:200]}...")
            except Exception as e:
                print(f"❌ Failed: {e}")

            # Test 3: Search tables
            print("\n🔎 Test 3: Searching for 'customer' tables...")
            print("-" * 40)
            try:
                result = await session.call_tool(
                    "search_tables", arguments={"search_term": "customer"}
                )
                content = result.content[0].text if result.content else "No content"
                # Parse and display nicely
                try:
                    data = json.loads(content)
                    if "tables" in data:
                        print(f"✅ Found {len(data['tables'])} matching tables")
                        for table in data["tables"][:3]:  # Show first 3
                            print(
                                f"   - {table.get('database', 'N/A')}.{table.get('schema', 'N/A')}.{table.get('name', 'N/A')}"
                            )
                        if len(data["tables"]) > 3:
                            print(f"   ... and {len(data['tables']) - 3} more")
                    else:
                        print(f"✅ Result: {content[:200]}...")
                except Exception:
                    print(f"✅ Result: {content[:200]}...")
            except Exception as e:
                print(f"❌ Failed: {e}")

            # Test 4: Execute a simple query
            print("\n📊 Test 4: Executing 'SHOW DATABASES' query...")
            print("-" * 40)
            try:
                result = await session.call_tool(
                    "execute_query", arguments={"sql": "SHOW DATABASES"}
                )
                content = result.content[0].text if result.content else "No content"
                # Parse and display nicely
                try:
                    data = json.loads(content)
                    if "rows" in data:
                        print(f"✅ Query returned {len(data['rows'])} rows")
                        for row in data["rows"][:3]:  # Show first 3
                            print(f"   {row}")
                        if len(data["rows"]) > 3:
                            print(f"   ... and {len(data['rows']) - 3} more")
                    else:
                        print(f"✅ Result: {content[:200]}...")
                except Exception:
                    print(f"✅ Result: {content[:200]}...")
            except Exception as e:
                print(f"❌ Failed: {e}")

            # Test 5: Try a write operation (should fail)
            print("\n🚫 Test 5: Testing write protection (should fail)...")
            print("-" * 40)
            try:
                result = await session.call_tool(
                    "execute_query",
                    arguments={"sql": "INSERT INTO test_table VALUES (1, 'test')"},
                )
                content = result.content[0].text if result.content else "No content"
                # Check if the result is an error
                try:
                    data = json.loads(content)
                    if data.get("status") == "error":
                        print(
                            f"✅ Correctly blocked: {data.get('message', 'Unknown error')[:100]}..."
                        )
                    else:
                        print("❌ Unexpected success! This should have been blocked")
                        print(f"   Result: {content}")
                except Exception:
                    print(f"❌ Unexpected result format: {content}")
            except Exception as e:
                print(f"✅ Correctly blocked (exception): {str(e)[:100]}...")

            print("\n" + "=" * 60)
            print("✨ All tests completed!")

    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_mcp_server())
