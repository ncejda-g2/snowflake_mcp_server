"""Tests for tools utilities."""

import json

import pytest

from server.tools.utils import parse_filters


class TestParseFilters:
    """Test cases for parse_filters function."""

    def test_parse_filters_empty_string(self):
        """Test parsing empty string filters."""
        result = parse_filters("test_prefix", "")
        assert result == {}

    def test_parse_filters_none(self):
        """Test parsing None filters."""
        result = parse_filters("test_prefix", None)
        assert result == {}

    def test_parse_filters_whitespace_only(self):
        """Test parsing whitespace-only filters."""
        result = parse_filters("test_prefix", "   ")
        assert result == {}

    def test_parse_filters_valid_json(self):
        """Test parsing valid JSON filters."""
        filters_json = '{"signal_type_cont": "page_view", "company_name_eq": "Acme"}'
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[signal_type_cont]": "page_view",
            "filters[company_name_eq]": "Acme",
        }
        assert result == expected

    def test_parse_filters_different_key_prefix(self):
        """Test parsing filters with different key prefix."""
        filters_json = '{"status_eq": "active", "type_cont": "software"}'
        result = parse_filters("dimension_filters", filters_json)

        expected = {
            "dimension_filters[status_eq]": "active",
            "dimension_filters[type_cont]": "software",
        }
        assert result == expected

    def test_parse_filters_with_empty_values(self):
        """Test parsing filters with empty values (should be excluded)."""
        filters_json = '{"field1_eq": "value1", "field2_eq": "", "field3_eq": "value3", "field4_eq": null}'
        result = parse_filters("filters", filters_json)

        expected = {"filters[field1_eq]": "value1", "filters[field3_eq]": "value3"}
        assert result == expected

    def test_parse_filters_with_numeric_values(self):
        """Test parsing filters with numeric values."""
        filters_json = '{"age_gt": 25, "score_lte": 100.5, "count_eq": 0}'
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[age_gt]": 25,
            "filters[score_lte]": 100.5,
            # count_eq with value 0 should be excluded (falsy)
        }
        assert result == expected

    def test_parse_filters_with_boolean_values(self):
        """Test parsing filters with boolean values."""
        filters_json = '{"is_active_eq": true, "is_deleted_eq": false}'
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[is_active_eq]": True
            # is_deleted_eq with value false should be excluded (falsy)
        }
        assert result == expected

    def test_parse_filters_with_list_values(self):
        """Test parsing filters with list values."""
        filters_json = (
            '{"categories_in": ["software", "tools"], "tags_cont": ["python", "web"]}'
        )
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[categories_in]": ["software", "tools"],
            "filters[tags_cont]": ["python", "web"],
        }
        assert result == expected

    def test_parse_filters_with_nested_objects(self):
        """Test parsing filters with nested object values."""
        filters_json = (
            '{"metadata_eq": {"key": "value"}, "config_cont": {"setting": "enabled"}}'
        )
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[metadata_eq]": {"key": "value"},
            "filters[config_cont]": {"setting": "enabled"},
        }
        assert result == expected

    def test_parse_filters_invalid_json(self):
        """Test parsing invalid JSON filters."""
        invalid_json = '{"invalid": json}'

        with pytest.raises(ValueError, match="filters must be valid JSON"):
            parse_filters("filters", invalid_json)

    def test_parse_filters_malformed_json(self):
        """Test parsing malformed JSON filters."""
        malformed_json = '{"field1": "value1", "field2":}'

        with pytest.raises(ValueError, match="filters must be valid JSON"):
            parse_filters("filters", malformed_json)

    def test_parse_filters_non_object_json(self):
        """Test parsing non-object JSON (should work if it's valid JSON)."""
        # Test with JSON array
        array_json = '["item1", "item2"]'
        with pytest.raises(AttributeError):
            # This should fail because arrays don't have .items()
            parse_filters("filters", array_json)

    def test_parse_filters_with_special_characters(self):
        """Test parsing filters with special characters in keys and values."""
        filters_json = '{"field_with_spaces_eq": "value with spaces", "field@symbol_cont": "value@symbol"}'
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[field_with_spaces_eq]": "value with spaces",
            "filters[field@symbol_cont]": "value@symbol",
        }
        assert result == expected

    def test_parse_filters_with_unicode_characters(self):
        """Test parsing filters with Unicode characters."""
        filters_json = '{"name_eq": "José González", "city_cont": "São Paulo"}'
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[name_eq]": "José González",
            "filters[city_cont]": "São Paulo",
        }
        assert result == expected

    def test_parse_filters_empty_object(self):
        """Test parsing empty JSON object."""
        empty_json = "{}"
        result = parse_filters("filters", empty_json)

        assert result == {}

    def test_parse_filters_large_json(self):
        """Test parsing large JSON with many filters."""
        filters_dict = {f"field_{i}_eq": f"value_{i}" for i in range(100)}
        filters_json = json.dumps(filters_dict)

        result = parse_filters("filters", filters_json)

        expected = {f"filters[field_{i}_eq]": f"value_{i}" for i in range(100)}
        assert result == expected

    def test_parse_filters_key_prefix_variations(self):
        """Test parsing with various key prefix formats."""
        filters_json = '{"test_eq": "value"}'

        # Test different prefix formats
        prefixes = [
            "simple",
            "complex_prefix",
            "prefix.with.dots",
            "prefix_with_underscores",
            "prefix-with-hyphens",
        ]

        for prefix in prefixes:
            result = parse_filters(prefix, filters_json)
            expected = {f"{prefix}[test_eq]": "value"}
            assert result == expected

    def test_parse_filters_preserves_json_types(self):
        """Test that parsing preserves JSON data types."""
        filters_json = '{"string_eq": "text", "number_eq": 42, "float_eq": 3.14, "bool_eq": true, "array_eq": [1, 2, 3]}'
        result = parse_filters("filters", filters_json)

        expected = {
            "filters[string_eq]": "text",
            "filters[number_eq]": 42,
            "filters[float_eq]": 3.14,
            "filters[bool_eq]": True,
            "filters[array_eq]": [1, 2, 3],
        }
        assert result == expected

    def test_parse_filters_json_decode_error_chaining(self):
        """Test that JSONDecodeError is properly chained as the cause."""
        invalid_json = '{"invalid": json}'

        with pytest.raises(ValueError) as exc_info:
            parse_filters("filters", invalid_json)

        # Check that the original JSONDecodeError is chained
        assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)
        assert "filters must be valid JSON" in str(exc_info.value)
