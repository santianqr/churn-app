from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Churn Predictions API"
    app_version: str = "0.1.0"
    debug: bool = False
    api_prefix: str = "/api/v1"
    allowed_origins: list[str] = ["*"]

    model_config = {"env_prefix": "CHURN_"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
