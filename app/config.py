from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = Field(default="development", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    unicommerce_base_url: str = Field(alias="UNICOMMERCE_BASE_URL")
    unicommerce_facility: str = Field(default="namhyafood", alias="UNICOMMERCE_FACILITY")
    unicommerce_wsdl_url: str = Field(alias="UNICOMMERCE_WSDL_URL")
    unicommerce_username: str = Field(alias="UNICOMMERCE_USERNAME")
    unicommerce_password: str = Field(alias="UNICOMMERCE_PASSWORD")
    unicommerce_timeout_seconds: float = Field(default=60.0, alias="UNICOMMERCE_TIMEOUT_SECONDS")
    unicommerce_max_retries: int = Field(default=3, alias="UNICOMMERCE_MAX_RETRIES")
    unicommerce_retry_backoff_seconds: float = Field(default=2.0, alias="UNICOMMERCE_RETRY_BACKOFF_SECONDS")

    page_size: int = Field(default=50, alias="PAGE_SIZE")
    max_pages: int = Field(default=10, alias="MAX_PAGES")
    shipment_delay_threshold_hours: int = Field(default=24, alias="SHIPMENT_DELAY_THRESHOLD_HOURS")
    low_stock_threshold: int = Field(default=10, alias="LOW_STOCK_THRESHOLD")

    # Comma-separated allowed hosts for MCP DNS rebinding protection.
    # Set to "*" to allow all (safe behind Render/Cloudflare HTTPS).
    mcp_allowed_hosts: str = Field(default="*", alias="MCP_ALLOWED_HOSTS")


@lru_cache
def get_settings() -> Settings:
    return Settings()
