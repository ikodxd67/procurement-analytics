"""Дымовые тесты настроек: проверяем, что конфиг собирается и читает окружение."""

from __future__ import annotations

import pytest

from procurement.config import Settings, SourceKind


def test_defaults_are_usable() -> None:
    settings = Settings()

    assert settings.source.kind is SourceKind.FIXTURES
    assert settings.source.base_url.endswith("/v3")
    assert settings.http.concurrency_min <= settings.http.concurrency_start
    assert settings.http.concurrency_start <= settings.http.concurrency_max
    assert settings.pipeline.queue_maxsize >= 1


def test_nested_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PA_SOURCE__KIND", "rest")
    monkeypatch.setenv("PA_HTTP__CONCURRENCY_START", "7")
    monkeypatch.setenv("PA_PIPELINE__QUEUE_MAXSIZE", "11")

    settings = Settings(_env_file=None)

    assert settings.source.kind is SourceKind.REST
    assert settings.http.concurrency_start == 7
    assert settings.pipeline.queue_maxsize == 11


def test_token_is_not_leaked_by_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PA_SOURCE__TOKEN", "super-secret-token")

    settings = Settings(_env_file=None)

    assert "super-secret-token" not in repr(settings)
    assert settings.source.token.get_secret_value() == "super-secret-token"
