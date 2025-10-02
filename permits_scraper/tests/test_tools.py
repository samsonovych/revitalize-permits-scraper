"""Unit tests for tools and registry.

These tests mock external calls and verify basic behavior of tool classes.
"""

from __future__ import annotations

from typing import Any

from osint_agent.tools.base import ToolRegistry, BaseTool
from osint_agent.schemas import ToolResult


class EchoTool(BaseTool):
    """Simple echo tool for testing."""

    name = "echo"
    description = "Echo the input"

    def invoke(self, query: str, **kwargs: Any) -> ToolResult:  # type: ignore[override]
        """Echo the query back to the caller."""
        return ToolResult(content=query, source="echo")


def test_registry_lookup_and_invoke() -> None:
    """Registry should find tool and return echoed content."""
    registry = ToolRegistry(tools=[EchoTool()])
    assert "echo" in registry.names()
    result = registry.invoke("echo", "hello")
    assert result.content == "hello"
