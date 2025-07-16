import os
from functools import lru_cache
from typing import Any, Optional, Union

from pydantic import AnyHttpUrl, BaseSettings, PostgresDsn, validator


class Settings(BaseSettings):
    PROJECT_NAME: str
    PROJECT_VERSION: str
    PROJECT_ENVIRONMENT: str
    BACKEND_CORS_ORIGINS: list[AnyHttpUrl] = []
    JWT_ALGORITHM: str = "RS256"

    PROJECT_PATH: str = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    GZIP_MINIMUM_SIZE: int = 500
    SENTRY: bool = False
    SENTRY_DSN: str
    REDIS: bool = False
    REDIS_HOST: str

    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, list[str]]) -> Union[list[str], str]:
        # pylint: disable=no-self-argument
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASS: str
    DB_NAME: str
    DATABASE_URI: Optional[PostgresDsn] = None

    DB_ORA_DSN: str
    DB_ORA_HOST: str
    DB_ORA_PORT: str
    DB_ORA_USER: str
    DB_ORA_PASS: str
    DB_ORA_NAME: str
    # DATABASE_ORA_URI: str

    @validator("DATABASE_URI", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: dict[str, Any]) -> Any:
        # pylint: disable=no-self-argument
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            user=values.get("DB_USER"),
            password=values.get("DB_PASS"),
            host=values.get("DB_HOST"),
            port=values.get("DB_PORT", "5432"),
            path=f"/{values.get('DB_NAME') or ''}",
        )

    # @validator("DATABASE_ORA_URI", pre=True)
    # def assemble_db_ora_connection(cls, v: Optional[str], values: dict[str, Any]) -> Any:
    #     # pylint: disable=no-self-argument
    #     if isinstance(v, str):
    #         return v
    #     return values.get("DB_ORA_DSN")

    class Config:
        case_sensitive = True
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    """
    Получение и кэширование настроек проекта
    Можно использовать как зависимость внутри эндпойнта:
        from app.core import config
        settings: config.Settings = Depends(config.get_settings)
    """
    return Settings()


settings = get_settings()
