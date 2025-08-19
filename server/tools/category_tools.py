import httpx
from fastmcp.contrib.mcp_mixin import MCPMixin, mcp_tool

from server.log_utils import setup_logging
from server.tools.auth_utils import get_auth_headers, merge_headers

logging = setup_logging()


class CategoryTools(MCPMixin):
    def __init__(self, client: httpx.AsyncClient):
        super().__init__()
        self.client = client

    @mcp_tool(
        name="list_categories",
        description="""
        This endpoint retrieves a list of categories.
        Parameters:
        - filter_name_eq: Filter categories by exact name.
        - filter_name_cont: Filter categories by name containing this value.
        - filter_slug_eq: Filter categories by exact slug.
        - filter_slug_cont: Filter categories by slug containing this value.
        - filter_created_at_gt: Filter categories created after this date.
        - filter_created_at_lt: Filter categories created before this date.
        - filter_updated_at_gt: Filter categories updated after this date.
        - filter_updated_at_lt: Filter categories updated before this date.
        - fields: Comma-separated list of category fields to include in the response.
            Available fields are name, slug, description, created_at, updated_at.
        - relationships: Comma-separated list of related resources to include (e.g., products, discussions).
        """,
    )
    async def list_categories(
        self,
        filter_name_eq: str | None = "",
        filter_name_cont: str | None = "",
        filter_slug_eq: str | None = "",
        filter_slug_cont: str | None = "",
        filter_created_at_gt: str | None = "",
        filter_created_at_lt: str | None = "",
        filter_updated_at_gt: str | None = "",
        filter_updated_at_lt: str | None = "",
        fields: str | None = "",
        relationships: str | None = "",
    ) -> dict:
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            "/api/v2/categories",
            params={
                "filter[name_eq]": filter_name_eq,
                "filter[name_cont]": filter_name_cont,
                "filter[slug_eq]": filter_slug_eq,
                "filter[slug_cont]": filter_slug_cont,
                "filter[created_at_gt]": filter_created_at_gt,
                "filter[created_at_lt]": filter_created_at_lt,
                "filter[updated_at_gt]": filter_updated_at_gt,
                "filter[updated_at_lt]": filter_updated_at_lt,
                "fields[categories]": fields,
                "include": relationships,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()

    @mcp_tool(
        name="show_category",
        description="""
        This endpoint retrieves details for a specific category by its ID.
        Parameters:
        - id: The ID of the category to retrieve.
        - fields: Comma-separated list of category fields to include in the response.
            Available fields are name, slug, description, created_at, updated_at.
        - relationships: Comma-separated list of related resources to include (e.g., products, discussions).
        """,
    )
    async def show_category(
        self,
        id: str,
        fields: str | None = "",
        relationships: str | None = "",
    ) -> dict:
        """Show a category."""
        # Get auth headers within tool call context
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            f"/api/v2/categories/{id}",
            params={
                "fields[categories]": fields,
                "include": relationships,
            },
            headers=headers,
        )
        response.raise_for_status()
        return response.json()
