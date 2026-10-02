"""Tests for startup dependency waiting.

The retry must distinguish "not up yet" (retry) from "broken" (raise now),
which mirrors the fail-closed policy used for backend selection.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings  # noqa: E402
from app.startup import retry_async  # noqa: E402


def _settings(**kw) -> Settings:
    base = {
        "startup_max_attempts": 3,
        "startup_backoff_seconds": 0.01,
        "startup_fail_fast": True,
    }
    base.update(kw)
    return Settings(**base)


async def test_returns_value_on_first_success() -> None:
    async def ok():
        return "ready"

    assert await retry_async("dep", ok, settings=_settings()) == "ready"


async def test_retries_transient_then_succeeds() -> None:
    """A slow-booting dependency is waited for, not treated as a failure."""
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionRefusedError("connection refused")
        return "ready"

    assert await retry_async("dep", flaky, settings=_settings()) == "ready"
    assert calls["n"] == 3


async def test_exhaustion_raises_when_required() -> None:
    async def down():
        raise ConnectionRefusedError("nope")

    with pytest.raises(RuntimeError, match="unreachable"):
        await retry_async("dep", down, settings=_settings())


async def test_exhaustion_returns_none_when_optional() -> None:
    """Optional dependencies degrade instead of killing the process."""
    calls = {"n": 0}

    async def down():
        calls["n"] += 1
        raise ConnectionRefusedError("nope")

    assert (
        await retry_async("dep", down, settings=_settings(), required=False)
        is None
    )
    assert calls["n"] == 3, "must exhaust the budget before giving up"


async def test_non_transient_error_raises_immediately() -> None:
    """A real bug must surface at once, not after N retries."""
    calls = {"n": 0}

    async def broken():
        calls["n"] += 1
        raise AttributeError("no attribute 'stats'")

    with pytest.raises(AttributeError):
        await retry_async("dep", broken, settings=_settings())
    assert calls["n"] == 1, "a bug must not be retried"


async def test_client_error_is_not_retried() -> None:
    """Invalid Cypher is a defect, not a slow start."""
    from neo4j.exceptions import ClientError

    calls = {"n": 0}

    async def bad_query():
        calls["n"] += 1
        raise ClientError("Invalid query.")

    with pytest.raises(ClientError):
        await retry_async("dep", bad_query, settings=_settings())
    assert calls["n"] == 1


async def test_extra_retryable_extends_the_set() -> None:
    class CustomDown(Exception):
        pass

    calls = {"n": 0}

    async def down():
        calls["n"] += 1
        if calls["n"] < 2:
            raise CustomDown("warming up")
        return "ready"

    result = await retry_async(
        "dep", down, settings=_settings(), extra_retryable=(CustomDown,)
    )
    assert result == "ready"
    assert calls["n"] == 2