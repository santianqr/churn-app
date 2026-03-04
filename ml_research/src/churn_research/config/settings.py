from __future__ import annotations

from typing import Any

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    """Snowflake connection settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="SNOWFLAKE_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
    )

    account: str
    user: str
    password: SecretStr
    warehouse: str
    database: str
    schema_name: str = Field(default="PUBLIC", validation_alias="snowflake_schema")
    role: str | None = None
    region: str | None = None
    login_timeout: int = 60
    network_timeout: int = 120

    def connection_params(self) -> dict[str, Any]:
        """Build connection parameters dict for snowflake.connector.connect()."""
        params: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_name,
            "password": self.password.get_secret_value(),
            "login_timeout": self.login_timeout,
            "network_timeout": self.network_timeout,
        }
        if self.role is not None:
            params["role"] = self.role
        if self.region is not None:
            params["region"] = self.region
        return params
