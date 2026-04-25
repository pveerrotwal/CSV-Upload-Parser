from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg2://postgres:postgres@db:5432/csv_processor"
    redis_url: str = "redis://redis:6379/0"
    data_dir: str = "data"
    poll_interval_ms: int = 2000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
