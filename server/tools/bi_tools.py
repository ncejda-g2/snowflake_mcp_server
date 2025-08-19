import httpx
from fastmcp.contrib.mcp_mixin import MCPMixin, mcp_tool

from server.log_utils import logger
from server.tools.auth_utils import get_auth_headers, merge_headers


class BuyerIntentTools(MCPMixin):
    def __init__(self, client: httpx.AsyncClient):
        super().__init__()
        self.client = client

    @mcp_tool(
        name="browse_buyer_intent_interactions",
        description="""
        Browse buyer intent interactions for a specific product.
        This endpoint uses an OLAP-style query language that supports:
        Parameters:
        - subject_product_id: The ID of the product to retrieve interactions for.
        - dimensions: Comma-separated list of dimensions to group by.
        - measures: Comma-separated list of measures to aggregate.
        - dimension_filters: JSON string of filters to apply to the dimensions.
        - include: Comma-separated list of related resources to include (e.g., users, products).
        - sort: Field to sort by (prefix with - for descending).
        Dimension Filters:
        - Use JSON format to specify filters for dimensions.
        - Example: '{"company_name_cont": "Acme", "day_gteq": "2024-01-01"}'
        - Supported operators:
            - _eq: Equals (exact match)
            - _not_eq: Not equals
            - _cont: Contains (substring match)
            - _not_cont: Does not contain
            - _gt: Greater than
            - _gteq: Greater than or equal
            - _lt: Less than
            - _lteq: Less than or equal
            - _present: Field has a value
            - _empty: Field is empty
        Example:
        dimension_filters='{"company_name_cont": "Acme", "day_gteq": "2024-01-01"}'
        dimension_filters='{"signal_type_eq": "page_view", "company_intent_score_gteq": "50"}'
        """,
    )
    async def browse_buyer_intent_interactions_tool(
        self,
        subject_product_id: str,
        dimensions: str | None = "",
        measures: str | None = "",
        dimension_filters: str | None = "",
        include: str | None = "",
        sort: str | None = "",
    ) -> dict:
        import json

        # Build params dict starting with basic parameters
        params = {
            "dimensions": dimensions,
            "measures": measures,
            "sort": sort,
            "include": include,
        }

        if dimension_filters and dimension_filters.strip():
            try:
                filters_dict = json.loads(dimension_filters)
                for filter_key, filter_value in filters_dict.items():
                    if filter_value:  # Only add non-empty values
                        params[f"dimension_filters[{filter_key}]"] = filter_value
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in dimension_filters: {dimension_filters}")
                raise ValueError(f"dimension_filters must be valid JSON: {e}") from e

        auth_headers = get_auth_headers()
        headers = merge_headers({"Accept": "application/json"}, auth_headers)
        response = await self.client.request(
            "GET",
            f"/api/v2/products/{subject_product_id}/buyer_intent",
            params=params,
            headers=headers,
        )

        response.raise_for_status()
        return response.json()
