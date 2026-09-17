"""Настройки приложения.

Все значения читаются из переменных окружения с префиксом ``PA_`` и
вложенностью через двойное подчёркивание: ``PA_HTTP__CONCURRENCY_START``.
Образец — в ``.env.example``.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceKind(StrEnum):
    REST = "rest"
    FIXTURES = "fixtures"


class SourceSettings(BaseModel):
    kind: SourceKind = SourceKind.FIXTURES
    base_url: str = "https://ows.goszakup.gov.kz/v3"
    token: SecretStr = SecretStr("")
    page_limit: int = Field(default=50, ge=1, le=500)
    fixtures_dir: str = "tests/fixtures/goszakup"


class HttpSettings(BaseModel):
    concurrency_start: int = Field(default=4, ge=1)
    concurrency_min: int = Field(default=1, ge=1)
    concurrency_max: int = Field(default=16, ge=1)
    request_timeout_s: float = Field(default=30.0, gt=0)
    connect_timeout_s: float = Field(default=10.0, gt=0)
    retry_max_attempts: int = Field(default=5, ge=1)
    retry_base_delay_s: float = Field(default=0.5, gt=0)
    retry_max_delay_s: float = Field(default=60.0, gt=0)
    user_agent: str = "procurement-analytics/0.1 (portfolio project)"


class PipelineSettings(BaseModel):
    queue_maxsize: int = Field(default=32, ge=1)
    batch_size: int = Field(default=500, ge=1)
    parser_workers: int = Field(default=2, ge=1)
    overall_timeout_s: float = Field(default=3600.0, gt=0)


class PostgresSettings(BaseModel):
    dsn: str = "postgresql+asyncpg://procurement:procurement@localhost:5432/procurement"
    pool_size: int = Field(default=5, ge=1)
    max_overflow: int = Field(default=5, ge=0)
    echo: bool = False


class ClickHouseSettings(BaseModel):
    host: str = "localhost"
    port: int = 8123
    database: str = "procurement"
    user: str = "default"
    password: SecretStr = SecretStr("")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PA_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Literal["local", "ci", "prod"] = "local"
    log_level: str = "INFO"

    source: SourceSettings = SourceSettings()
    http: HttpSettings = HttpSettings()
    pipeline: PipelineSettings = PipelineSettings()
    postgres: PostgresSettings = PostgresSettings()
    clickhouse: ClickHouseSettings = ClickHouseSettings()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
