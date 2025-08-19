import json


def parse_filters(key_prefix: str, filters: str | None) -> dict[str, str]:
    """Parse filters JSON string into query parameters.

    Args:
        key_prefix: The prefix for parameter keys (e.g., "dimension_filters", "filters")
        filters: JSON string of filters where keys are in format "field_operator"
                 (e.g., '{"signal_type_cont": "page_view", "company_name_eq": "Acme"}')

    Returns:
        Dictionary of query parameters with key_prefix[] format

    Raises:
        ValueError: If filters is not valid JSON
    """
    if not filters or not filters.strip():
        return {}

    try:
        filters_dict = json.loads(filters)
        return {
            f"{key_prefix}[{filter_key}]": filter_value
            for filter_key, filter_value in filters_dict.items()
            if filter_value  # Only add non-empty values
        }
    except json.JSONDecodeError as e:
        raise ValueError(f"filters must be valid JSON: {e}") from e
