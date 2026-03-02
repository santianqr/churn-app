from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Churn Predictions API"
    app_version: str = "0.1.0"
    debug: bool = False
    api_prefix: str = "/api/v1"

    model_config = {"env_prefix": "CHURN_"}


settings = Settings()
