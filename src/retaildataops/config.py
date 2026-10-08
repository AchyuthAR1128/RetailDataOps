"""Application configuration loaded from environment variables.

Uses pydantic-settings to provide type-safe, validated configuration.
Values are loaded from environment variables and/or a .env file.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """RetailDataOps application settings.

    All values can be overridden via environment variables.
    For local development, create a .env file from .env.example.
    """

    model_config = SettingsConfigDict(
        env_prefix="RETAILDATAOPS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # -- Application ----------------------------------------------------------
    app_name: str = "RetailDataOps"
    app_env: str = "development"  # development | staging | production
    debug: bool = False
    log_level: str = "INFO"


def get_settings() -> Settings:
    """Create and return a validated Settings instance.

    Returns:
        A fully validated Settings object with values loaded from
        environment variables and/or a .env file.
    """
    return Settings()
