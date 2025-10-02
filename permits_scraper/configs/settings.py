"""Configuration module for the OSINT Agent.

This module defines application settings using Pydantic's settings management.
It loads environment variables via ``python-dotenv`` to simplify local development.
"""

from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


load_dotenv(override=True)


class Settings(BaseSettings):
    """
    Settings class for the Permits Scraper.

    Extended configuration for server.

    Parameters
    ----------
    API_HOST : str, default="0.0.0.0"
        Host address for the OSINT API server.
    API_PORT : int, default=8000
        Port for the OSINT API server.


    Returns
    -------
    Settings
        A validated settings object.

    See Also
    --------
    BaseSettings : Pydantic settings base class for environment variable loading.

    Examples
    --------
    >>> from osint_agent.configs.settings import Settings
    >>> settings = Settings()
    >>> settings.API_HOST
    '0.0.0.0'
    """

    API_HOST: str = Field(default="0.0.0.0", description="Host address for the OSINT API server")
    API_PORT: int = Field(default=8000, description="Port for the OSINT API server")


# Create a global instance of the settings
app_config = Settings()
