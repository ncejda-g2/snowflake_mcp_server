import httpx
from fastmcp.contrib.mcp_mixin import MCPMixin, mcp_tool

from server.log_utils import setup_logging
from server.tools.auth_utils import get_auth_headers, merge_headers

logging = setup_logging()


class ProductTools(MCPMixin):
    def __init__(self, client: httpx.AsyncClient):
        super().__init__()
        self.client = client

    @mcp_tool(
        name="list_products",
        description="""
        This endpoint retrieves a list of products.
        Parameters:
        - filter_category_id: Filter products by category ID.
        - filter_product_id: Filter products by product ID.
        - filter_review_count_gteq: Filter products with review count greater than or equal to this value.
        - filter_slug: Filter products by slug.
        - filter_star_rating: Filter products by star rating.
        - filter_vendor_name: Filter products by vendor name.
        - fields: Comma-separated list of product fields to include in the response.
            Available detail_description, domain, g2_url, image_url, name, public_detail_url, review_count, slug, star_rating, write_review_url
        - relationships: Comma-separated list of related resources to include (e.g., categories, discussions, vendors).
        """,
    )
    async def list_products(
        self,
        filter_category_id: str | None = None,
        filter_product_id: str | None = None,
        filter_review_count_gteq: int | None = None,
        filter_slug: str | None = None,
        filter_star_rating: float | None = None,
        filter_vendor_name: str | None = None,
        fields: str | None = None,
        relationships: str | None = None,
    ) -> dict:
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            "/api/v2/products",
            params={
                "filter[category_id]": filter_category_id,
                "filter[product_id]": filter_product_id,
                "filter[review_count_gteq]": filter_review_count_gteq,
                "filter[slug]": filter_slug,
                "filter[star_rating]": filter_star_rating,
                "filter[vendor_name]": filter_vendor_name,
                "fields[products]": fields,
                "include": relationships,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()

    @mcp_tool(
        name="list_my_products",
        description="""
        This endpoint shows subscription-specific product information, of products that the current account has ownership of.
        Parameters:
        - product_fields: Comma-separated list of product fields to include in the response.
        Available fields are name, domain, slug, image_url.
        - relationships: Comma-separated list of related resources to include (e.g., categories, subscribed_categories).
        """,
    )
    async def list_my_products(
        self,
        product_fields: str | None = "",
        relationships: str | None = "",
    ) -> dict:
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            "/api/v2/users/me/products",
            params={
                "fields[products]": product_fields,
                "include": relationships,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()

    @mcp_tool(
        name="show_product",
        description="""
        This endpoint retrieves details for a specific product by its ID.
        Parameters:
        - id: The ID of the product to retrieve.
        - fields: Comma-separated list of product fields to include in the response.
            Available fields are name, domain, slug, image_url, description, detail_description, g2_url, public_detail_url, review_count, star_rating, write_review_url.
        - relationships: Comma-separated list of related resources to include (e.g., categories, discussions, vendors).
        """,
    )
    async def show_product(
        self,
        id: str,
        fields: str | None = "",
        relationships: str | None = "",
    ) -> dict:
        # Get auth headers within tool call context
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            f"/api/v2/products/{id}",
            params={
                "fields[products]": fields,
                "include": relationships,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()
