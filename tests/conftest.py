"""Pytest configuration and fixtures."""

import pytest

from server.config import Config


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return Config(
        base_url="https://test.g2.com/api/v1",
        timeout=10,
        user_agent="Test-Agent/1.0",
        debug=True,
        max_page_size=50,
    )


@pytest.fixture
def sample_product_data():
    """Sample product data for testing."""
    return {
        "data": [
            {
                "id": "1",
                "type": "product",
                "attributes": {
                    "name": "Test Product",
                    "slug": "test-product",
                    "description": "A test product for testing",
                    "logo": {"url": "https://example.com/logo.png"},
                    "vendor_name": "Test Vendor",
                    "categories": [{"name": "Test Category", "slug": "test-category"}],
                    "star_rating": 4.5,
                    "review_count": 100,
                },
            }
        ]
    }


@pytest.fixture
def sample_review_data():
    """Sample review data for testing."""
    return {
        "data": [
            {
                "id": "1",
                "type": "review",
                "attributes": {
                    "title": "Great product!",
                    "comment": "This product is amazing and works well.",
                    "star_rating": 5,
                    "created_at": "2024-01-01T00:00:00Z",
                    "reviewer": {"name": "John Doe", "title": "Engineer"},
                    "product": {"name": "Test Product"},
                },
            }
        ]
    }


@pytest.fixture
def sample_category_data():
    """Sample category data for testing."""
    return {
        "data": [
            {
                "id": "1",
                "type": "category",
                "attributes": {
                    "name": "Test Category",
                    "slug": "test-category",
                    "description": "A test category for testing",
                },
            }
        ]
    }
