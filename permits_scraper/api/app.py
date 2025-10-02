"""
FastAPI application configuration and router setup.

This module configures the main FastAPI application instance and includes
all routers for different service endpoints.
"""

from fastapi import FastAPI
from loguru import logger
from dspy import inspect_history

from osint_agent.agents.dspy_agent import DspyResearchAgent
from osint_agent.schemas import ResearchRequest, ResearchResponse


# Create FastAPI application instance
app = FastAPI(
    title="OSINT API",
    description="Conversational AI API for sales prospecting, voice interactions, document management, and pre-screening operations",
    version="0.1.0"
)


@app.get("/", tags=["Root"])
async def root():
    """Get available endpoints from app instance."""
    return {
        "message": "Welcome to OSINT API",
        "endpoints": [route.path for route in app.routes],
    }


@app.post("/research", tags=["Research"], response_model=ResearchResponse)
async def research(request: ResearchRequest) -> ResearchResponse:
    """Run the research agent and return a synthesized answer."""
    agent = DspyResearchAgent(max_steps=request.max_steps or DspyResearchAgent().max_steps)
    answer = agent.execute(request.query)
    logger.info(f"History: {inspect_history(n=5)}")
    return ResearchResponse(answer=answer)
