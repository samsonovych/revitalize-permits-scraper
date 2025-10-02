"""Basic tests for the DspyResearchAgent with a mocked planner/synthesizer."""

from __future__ import annotations

from typing import Any

import dspy
import pytest

from osint_agent.agents.dspy_agent import DspyResearchAgent
from osint_agent.tools.base import ToolRegistry, BaseTool
from osint_agent.schemas import ToolResult


class FixedPlan(dspy.Module):
    """Return a fixed plan string for testing."""

    def __init__(self, plan: str) -> None:
        """Store fixed plan."""
        super().__init__()
        self._plan = plan

    def forward(self, question: str, tools: str) -> Any:  # type: ignore[override]
        """Return object with 'plan' attribute."""
        return type("Out", (), {"plan": self._plan})


class FixedSynth(dspy.Module):
    """Return a fixed synthesized answer for testing."""

    def __init__(self, answer: str) -> None:
        """Store fixed answer."""
        super().__init__()
        self._answer = answer

    def forward(self, question: str, scratchpad: str) -> Any:  # type: ignore[override]
        """Return object with 'answer' attribute."""
        return type("Out", (), {"answer": self._answer})


class EchoTool(BaseTool):
    """Echo tool used in test ReAct trajectory."""

    name = "echo"
    description = "Echo the arg"

    def invoke(self, query: str, **kwargs: Any) -> ToolResult:  # type: ignore[override]
        """Return echoed content prefixed with 'echo: '."""
        return ToolResult(content=f"echo: {query}")


def test_agent_react_then_final(monkeypatch: pytest.MonkeyPatch) -> None:
    """Agent uses tool then synthesizes final answer."""
    agent = DspyResearchAgent(max_steps=2, registry=ToolRegistry(tools=[EchoTool()]))
    agent._planner = FixedPlan("use echo: test")  # type: ignore[attr-defined]
    agent._synthesizer = FixedSynth("final answer")  # type: ignore[attr-defined]
    answer = agent.execute("q")
    assert answer == "final answer"
