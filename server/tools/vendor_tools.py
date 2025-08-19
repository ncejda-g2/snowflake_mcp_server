import httpx
from fastmcp.contrib.mcp_mixin import MCPMixin, mcp_tool

from server.tools.auth_utils import get_auth_headers, merge_headers


class VendorTools(MCPMixin):
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    @mcp_tool(
        name="list_vendors",
        description="""
    This endpoint retrieves a list of vendors.
        Parameters:
        - updated_at_gt: Filter vendors updated after this timestamp (rfc3339 format).
        - updated_at_lt: Filter vendors updated before this timestamp (rfc3339 format).
        - fields: Comma-separated list of vendor fields to include in the response.
          Available fields are name, description, company_website, slug, public_products_count, updated_at
        - relationships: Comma-separated list of related resources to include (e.g., products
    """,
    )
    async def list_vendors(
        self,
        updated_at_gt: str | None = "",
        updated_at_lt: str | None = "",
        fields: str | None = "",
        relationships: str | None = "",
    ) -> dict:
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            "/api/v2/vendors",
            params={
                "filter[updated_at_gt]": updated_at_gt,
                "filter[updated_at_lt]": updated_at_lt,
                "fields[vendors]": fields,
                "include": relationships,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()

    @mcp_tool(
        name="show_vendor",
        description="""
        Show details for a specific vendor.
        This endpoint retrieves details for a vendor by its ID.
        Parameters:
        - id: The ID of the vendor to retrieve.
        - fields: Comma-separated list of vendor fields to include in the response.
          Available fields are name, description, company_website, slug, public_products_count, updated_at
        - relationships: Comma-separated list of related resources to include (e.g., products).

    """,
    )
    async def show_vendor(
        self,
        id: str,
        fields: str | None = "",
        relationships: str | None = "",
    ) -> dict:
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            f"/api/v2/vendors/{id}",
            params={
                "fields[vendors]": fields,
                "include": relationships,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()
