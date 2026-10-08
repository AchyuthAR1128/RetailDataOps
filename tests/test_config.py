"""Smoke tests for configuration loading."""

from __future__ import annotations

import pytest

from retaildataops.config import Settings, get_settings


class TestSettings:
    """Tests for the Settings configuration class."""

    def test_default_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Settings can be instantiated with all defaults."""
        # Clear any prefixed env vars that could interfere
        for key in (
            "RETAILDATAOPS_APP_NAME",
            "RETAILDATAOPS_APP_ENV",
            "RETAILDATAOPS_DEBUG",
            "RETAILDATAOPS_LOG_LEVEL",
        ):
            monkeypatch.delenv(key, raising=False)
        settings = Settings()
        assert settings.app_name == "RetailDataOps"
        assert settings.app_env == "development"
        assert settings.debug is False
        assert settings.log_level == "INFO"

    def test_get_settings_returns_settings_instance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """get_settings() returns a valid Settings instance."""
        monkeypatch.delenv("RETAILDATAOPS_DEBUG", raising=False)
        settings = get_settings()
        assert isinstance(settings, Settings)

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Environment variables override default values."""
        monkeypatch.setenv("RETAILDATAOPS_APP_ENV", "production")
        monkeypatch.setenv("RETAILDATAOPS_DEBUG", "true")
        monkeypatch.setenv("RETAILDATAOPS_LOG_LEVEL", "DEBUG")
        settings = Settings()
        assert settings.app_env == "production"
        assert settings.debug is True
        assert settings.log_level == "DEBUG"

    def test_app_name_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """RETAILDATAOPS_APP_NAME can be overridden via environment variable."""
        monkeypatch.delenv("RETAILDATAOPS_DEBUG", raising=False)
        monkeypatch.setenv("RETAILDATAOPS_APP_NAME", "TestApp")
        settings = Settings()
        assert settings.app_name == "TestApp"
