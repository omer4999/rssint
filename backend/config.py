"""
Application configuration loaded from environment variables via .env file.
All settings are validated and typed using Pydantic Settings.
"""

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central application settings backed by environment variables."""

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- Telegram ---
    telegram_api_id: int
    telegram_api_hash: str

    # --- Database ---
    database_url: str

    # --- Ingestion ---
    ingest_interval_seconds: int = 60
    enable_telegram_ingest: bool = True
    # --- OpenAI ---
    openai_api_key: str | None = None

    # --- Application ---
    app_env: str = "development"
    log_level: str = "INFO"

    @property
    def channels_file(self) -> Path:
        """Absolute path to the channels configuration file."""
        return Path(__file__).parent / "config" / "channels.txt"

    def load_channels(self) -> list[str]:
        """
        Load channel names from the channels config file.

        Each non-empty, non-comment line is treated as a channel identifier.
        Lines beginning with '#' are ignored.

        Returns:
            A list of channel name strings.
        """
        if not self.channels_file.exists():
            return []

        channels: list[str] = []
        for line in self.channels_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                channels.append(stripped)
        return channels


def get_settings() -> Settings:
    """Factory that returns a validated Settings instance."""
    return Settings()
