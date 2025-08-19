from typing import Literal

import httpx
from fastmcp.contrib.mcp_mixin import MCPMixin, mcp_tool

from server.tools.auth_utils import get_auth_headers, merge_headers


class ReviewTools(MCPMixin):
    def __init__(self, client: httpx.AsyncClient):
        super().__init__()
        self.client = client

    @mcp_tool(
        name="list_product_reviews",
        description="""
        This endpoint retrieves reviews for a product by its ID.
        Parameters:
        - product_id: The ID of the product to retrieve reviews for.
        - serializer: The type of serializer to use for the response. Options are "standard" or "market_intelligence". Defaults to "standard".
              - fields: Comma-separated list of review fields to include in the response.
                  If the serializer is "standard", available fields are answers, attribution, comments_present, country_name, default_sort is_public, official_response_present, percent_complete, product_name, published_at, regions, review_incentive, slug, source, star_rating, submitted_at, title, url, user_updated_at, verified_current_user.
                  If the serializer is "market_intelligence", available fields are answers, category_names, company_segment_name, country_code, country_name, feature_ratings, industry_name, primary_region_name, product_name, rating, switched_from_products, switched_theme, title, url, user_company_name, user_updated_at.
        - fields: Comma-separated list of review fields to include in the response.
            Available fields are answers, attribution, comments_present, country_name, default_sort is_public,
            official_response_present, percent_complete, product_name, published_at, regions, review_incentive,
            slug, source, star_rating, submitted_at, title, url, user_updated_at, verified_current_user.""",
    )
    async def list_product_reviews(
        self,
        product_id: str,
        serializer: Literal["standard", "market_intelligence"] | None = "standard",
        fields: str | None = "",
    ) -> dict:
        # Get auth headers within tool call context
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            f"/api/v2/products/{product_id}/reviews",
            params={
                "serializer": serializer,
                "fields[reviews]": fields,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()

    @mcp_tool(
        name="show_product_review",
        description="""
        This endpoint retrieves the review for a specific product owned by the current user by its ID.
        Parameters:
        - product_id: The ID of the product to retrieve the review for.
        - review_fields: Comma-separated list of review fields to include in the response.
            Available fields are questions, published_at, submitted_at, user_updated_at, url.
        - include: Comma-separated list of related resources to include (e.g., users, answers).
        """,
    )
    async def show_product_review(
        self,
        product_id: str,
        review_fields: str | None = "",
        include: str | None = "company,product,category",
    ) -> dict:
        # Get auth headers within tool call context
        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)

        response = await self.client.request(
            "GET",
            f"/api/v2/users/me/products/{product_id}/review",
            params={
                "fields[reviews]": review_fields,
                "include": include,
            },
            headers=headers,
        )

        response.raise_for_status()
        return response.json()
