"""
Application entry point and server startup module.

This module provides the main entry point for the OSINT Agent.
AI application. It configures and starts the FastAPI server using Uvicorn,
loading configuration from global settings and initializing the API endpoints.
"""

import uvicorn
from permits_scraper.configs.settings import app_config


def main() -> None:
    """
    Start the FastAPI application server.

    Initializes and runs the Uvicorn ASGI server with the FastAPI application,
    using host and port configuration from global application settings.
    This function serves as the primary entry point for the application.

    Examples
    --------
    >>> main()  # Starts the server on configured host and port
    """
    uvicorn.run(
        "permits_scraper.api.app:app",
        host=app_config.API_HOST,
        port=app_config.API_PORT,
        reload=True
    )


if __name__ == "__main__":
    main()
