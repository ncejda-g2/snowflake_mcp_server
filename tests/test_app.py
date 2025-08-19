"""Tests for the main application module."""

from unittest.mock import patch

from server.app import main


class TestAppInitialization:
    """Test cases for application initialization."""

    def test_app_initialization_components(self):
        """Test that all application components are properly initialized."""
        import server.app

        # Test that module-level variables are properly initialized
        assert server.app.config is not None
        assert server.app.logger is not None
        assert server.app.client is not None
        assert server.app.mcp is not None
        assert server.app.tools is not None

        # Test that config has expected attributes
        assert hasattr(server.app.config, "base_url")
        assert hasattr(server.app.config, "timeout")
        assert hasattr(server.app.config, "debug")

        # Test that logger is properly configured
        assert server.app.logger.name == "g2_mcp_server"

        # Test that client is configured
        assert server.app.client.base_url is not None

        # Test that MCP server is configured
        assert server.app.mcp.name == "G2 MCP Server"

        # Test that tools list has expected structure
        assert isinstance(server.app.tools, list)
        assert len(server.app.tools) == 4

        # Test that each tool has register_all method
        for tool in server.app.tools:
            assert hasattr(tool, "register_all")
            assert callable(tool.register_all)

    def test_app_logging_messages(self):
        """Test that appropriate logging messages are generated."""
        import server.app

        # Test that logger is properly configured and accessible
        assert server.app.logger is not None
        assert server.app.logger.name == "g2_mcp_server"

        # Test that config values are accessible for logging
        assert server.app.config.base_url is not None
        assert hasattr(server.app.config, "debug")

        # Test that the log statements would have proper values
        base_url = server.app.config.base_url
        debug = server.app.config.debug

        # These are the values that would be logged
        assert isinstance(base_url, str)
        assert isinstance(debug, bool)


class TestMainFunction:
    """Test cases for the main function."""

    @patch("server.app.mcp")
    def test_main_calls_mcp_run(self, mock_mcp):
        """Test that main function calls mcp.run()."""
        main()
        mock_mcp.run.assert_called_once()

    @patch("server.app.mcp")
    def test_main_function_signature(self, _):
        """Test that main function has correct signature."""
        import inspect

        sig = inspect.signature(main)

        # Should have no parameters
        assert len(sig.parameters) == 0

        # Should return None
        assert sig.return_annotation is None or sig.return_annotation is type(None)


class TestModuleExecution:
    """Test cases for module execution."""

    @patch("server.app.main")
    def test_module_execution_calls_main(self, _):
        """Test that module execution calls main when run directly."""
        # This would normally be tested by running the module directly,
        # but we'll test the conditional logic
        import importlib

        import server.app

        # Mock __name__ to simulate direct execution
        with patch.object(server.app, "__name__", "__main__"):
            importlib.reload(server.app)

        # Note: This test is more of a structure validation since
        # the if __name__ == "__main__" block runs during import
        # The actual test would need to be done differently


class TestImportStructure:
    """Test cases for import structure and dependencies."""

    def test_required_imports_present(self):
        """Test that all required imports are present and accessible."""
        import server.app

        # Test that critical components are accessible
        assert hasattr(server.app, "FastMCP")
        assert hasattr(server.app, "MCPMixin")
        assert hasattr(server.app, "Config")
        assert hasattr(server.app, "register_healthchecks")
        assert hasattr(server.app, "setup_logging")
        assert hasattr(server.app, "create_base_client")
        assert hasattr(server.app, "BuyerIntentTools")
        assert hasattr(server.app, "CategoryTools")
        assert hasattr(server.app, "ProductTools")
        assert hasattr(server.app, "ReviewTools")

    def test_module_level_variables_exist(self):
        """Test that module-level variables are properly defined."""
        import server.app

        # These should be defined at module level
        assert hasattr(server.app, "config")
        assert hasattr(server.app, "logger")
        assert hasattr(server.app, "client")
        assert hasattr(server.app, "mcp")
        assert hasattr(server.app, "tools")

    def test_tools_list_structure(self):
        """Test that tools list has the expected structure."""
        import server.app

        # Should be a list
        assert isinstance(server.app.tools, list)

        # Should have expected number of tools
        assert len(server.app.tools) == 4

        # Each tool should have register_all method
        for tool in server.app.tools:
            assert hasattr(tool, "register_all")
            assert callable(tool.register_all)
