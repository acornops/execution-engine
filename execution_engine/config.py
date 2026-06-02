"""Configuration management for the Execution Engine."""

from pathlib import Path
from urllib.parse import urlparse

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_ORCH_BASE_URL = "http://localhost:8000"
DEFAULT_ORCH_SERVICE_TOKEN = "default_token"
DEFAULT_DISPATCH_TOKEN = "default_dispatch_token"
DEFAULT_REDIS_URL = "redis://localhost:6379/1"


class Settings(BaseSettings):
    """Application settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(env_file=".env")

    APP_ENV: str = "development"
    ORCH_BASE_URL: str = DEFAULT_ORCH_BASE_URL
    ORCH_SERVICE_TOKEN: str = DEFAULT_ORCH_SERVICE_TOKEN
    EXECUTION_ENGINE_DISPATCH_TOKEN: str = DEFAULT_DISPATCH_TOKEN
    EXECUTION_GATEWAY_BASE_URL: str | None = None
    MAX_CONCURRENT_RUNS: int = 100
    HTTP_PORT: int = 8080
    INTERNAL_TRANSPORT_TLS_ENABLED: bool = False
    INTERNAL_TRANSPORT_TLS_REQUIRE_CLIENT_CERT: bool = True
    INTERNAL_TRANSPORT_TLS_CA_FILE: str | None = None
    INTERNAL_TRANSPORT_TLS_CERT_FILE: str | None = None
    INTERNAL_TRANSPORT_TLS_KEY_FILE: str | None = None
    INTERNAL_TRANSPORT_HEALTH_PORT: int | None = None

    EVENT_BATCH_SIZE: int = 50
    EVENT_FLUSH_INTERVAL_MS: int = 100
    MAX_EVENT_BUFFER: int = 2000
    DEFAULT_MAX_RUNTIME_MS: int = 300000
    REDIS_URL: str | None = None
    EXECUTION_DURABILITY_KEY_PREFIX: str = "execution-engine"
    TERMINAL_RUN_TTL_SECONDS: int = 3600
    TERMINAL_COMMIT_RETENTION_SECONDS: int = 86400
    TERMINAL_COMMIT_RETRY_INTERVAL_SECONDS: int = 5
    STALE_ACTIVE_RUN_RECOVERY_ON_STARTUP: bool = True
    READINESS_CHECK_TIMEOUT_MS: int = 1000
    RUN_ID_LOCK_TTL_SECONDS: int | None = None
    ORCH_RETRY_MAX_ELAPSED_SECONDS: int = 30
    GATEWAY_STREAM_IDLE_TIMEOUT_SECONDS: int = 60
    TOOL_CALL_TIMEOUT_SECONDS: int = 30
    TOOL_APPROVAL_TIMEOUT_SECONDS: int = 300
    DISPATCH_REQUEST_TIMEOUT_SECONDS: int = 10
    MAX_REQUEST_BODY_BYTES: int = 1_000_000
    LOG_LEVEL: str = "INFO"
    ENABLE_API_DOCS: bool = False

    @property
    def is_production(self) -> bool:
        """Returns true when production-only validation and behavior should apply."""
        return self.APP_ENV.lower() == "production"

    @property
    def durability_redis_url(self) -> str | None:
        """Returns the Redis URL used for durability."""
        return self.REDIS_URL

    @property
    def run_id_lock_ttl_seconds(self) -> int:
        """Returns a conservative lock TTL tied to the default runtime budget."""
        if self.RUN_ID_LOCK_TTL_SECONDS is not None:
            return self.RUN_ID_LOCK_TTL_SECONDS
        return max(int(self.DEFAULT_MAX_RUNTIME_MS / 1000) + 300, 600)

    @model_validator(mode="after")
    def validate_production_config(self) -> "Settings":
        """Reject development defaults when production mode is enabled."""
        if self.INTERNAL_TRANSPORT_TLS_ENABLED:
            self._validate_internal_transport_tls()

        if not self.is_production:
            return self

        if self.ORCH_SERVICE_TOKEN == DEFAULT_ORCH_SERVICE_TOKEN:
            raise ValueError("ORCH_SERVICE_TOKEN must be set to a non-default value in production")
        if self.EXECUTION_ENGINE_DISPATCH_TOKEN == DEFAULT_DISPATCH_TOKEN:
            raise ValueError("EXECUTION_ENGINE_DISPATCH_TOKEN must be set to a non-default value in production")
        if not self.durability_redis_url or self.durability_redis_url == DEFAULT_REDIS_URL:
            raise ValueError("REDIS_URL must be explicitly set in production")
        if self.ORCH_BASE_URL == DEFAULT_ORCH_BASE_URL:
            raise ValueError("ORCH_BASE_URL must be explicitly set in production")
        if not self.EXECUTION_GATEWAY_BASE_URL:
            raise ValueError("EXECUTION_GATEWAY_BASE_URL must be explicitly set in production")
        return self

    def _validate_internal_transport_tls(self) -> None:
        """Reject incomplete internal TLS configuration before serving traffic."""
        for field_name in ("INTERNAL_TRANSPORT_TLS_CERT_FILE", "INTERNAL_TRANSPORT_TLS_KEY_FILE"):
            path = getattr(self, field_name)
            if not path or not Path(path).is_file():
                raise ValueError(f"{field_name} must point to a readable file when internal transport TLS is enabled")
        if not self.INTERNAL_TRANSPORT_TLS_CA_FILE or not Path(self.INTERNAL_TRANSPORT_TLS_CA_FILE).is_file():
            raise ValueError(
                "INTERNAL_TRANSPORT_TLS_CA_FILE must point to a readable file when internal transport TLS is enabled"
            )
        for field_name in ("ORCH_BASE_URL", "EXECUTION_GATEWAY_BASE_URL"):
            value = getattr(self, field_name)
            if not value or urlparse(value).scheme != "https":
                raise ValueError(f"{field_name} must use https when internal transport TLS is enabled")


settings = Settings()
