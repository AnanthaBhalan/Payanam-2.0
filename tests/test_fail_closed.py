"""Regression tests for fail-closed backend selection.

A backend may be substituted with a test double for exactly one reason: the
real service is *unreachable*. Anything else -- a missing method, invalid
Cypher, a malformed response -- is a defect and must raise.

This matters because the blanket ``except Exception`` that used to guard
``get_repository()`` swallowed a ``NotImplementedError`` from a method that had
drifted out of its class, silently swapped in the in-memory double, and let the
entire suite pass green while the production path was broken.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.graph import repository as repo_mod  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_repo():
    repo_mod.set_repository(None)
    yield
    repo_mod.set_repository(None)


def test_unreachable_memgraph_falls_back(monkeypatch) -> None:
    """A genuinely offline server still degrades to the in-memory double."""
    from neo4j.exceptions import ServiceUnavailable

    class _OfflineClient:
        def __init__(self, settings=None) -> None:
            pass

        def connect(self) -> None:
            raise ServiceUnavailable("connection refused")

    monkeypatch.setattr(repo_mod, "MemgraphClient", _OfflineClient)

    repo = repo_mod.get_repository(refresh=True)
    assert isinstance(repo, repo_mod.InMemoryRepository)


@pytest.mark.parametrize(
    "error",
    [
        NotImplementedError("MemgraphRepository.stats is missing"),
        AttributeError("'MemgraphClient' object has no attribute 'stats'"),
        RuntimeError("query returned an unexpected shape"),
    ],
)
def test_broken_memgraph_raises_instead_of_degrading(monkeypatch, error) -> None:
    """A reachable-but-broken backend must raise, never silently degrade."""

    class _ConnectedClient:
        def __init__(self, settings=None) -> None:
            pass

        def connect(self) -> None:
            return None  # the server is up; the *code* is broken

    monkeypatch.setattr(repo_mod, "MemgraphClient", _ConnectedClient)

    def _explode(*_args, **_kwargs):
        raise error

    # Blow up wherever the repository touches the client.
    monkeypatch.setattr(repo_mod.MemgraphRepository, "stats", _explode)

    with pytest.raises(type(error)):
        repo_mod.get_repository(refresh=True)

    # Critically: no silent substitution happened.
    assert repo_mod._REPO is None, "a broken backend must not be swapped out"


def test_broken_subgraph_query_surfaces(monkeypatch) -> None:
    """An invalid Cypher error propagates rather than degrading."""

    class _ConnectedClient:
        def __init__(self, settings=None) -> None:
            pass

        def connect(self) -> None:
            return None

        def run(self, *_args, **_kwargs):
            from neo4j.exceptions import ClientError

            raise ClientError("Invalid query.")

    monkeypatch.setattr(repo_mod, "MemgraphClient", _ConnectedClient)
    monkeypatch.setattr(
        repo_mod.MemgraphRepository, "stats", lambda self: {"nodes": 5, "edges": 16}
    )

    repo = repo_mod.get_repository(refresh=True)
    assert repo.backend == "memgraph"
    with pytest.raises(Exception) as excinfo:
        repo.subgraph("MAS", 1)
    assert "Invalid query" in str(excinfo.value)


def test_force_memory_still_works(monkeypatch) -> None:
    """The explicit escape hatch is unaffected by the fail-closed policy."""

    class _ExplodingClient:
        def __init__(self, settings=None) -> None:
            raise AssertionError("must not be constructed")

    monkeypatch.setattr(repo_mod, "MemgraphClient", _ExplodingClient)
    repo = repo_mod.get_repository(force_memory=True, refresh=True)
    assert isinstance(repo, repo_mod.InMemoryRepository)