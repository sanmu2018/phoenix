"""Phase 2 caller-wiring integration tests for sandbox session reuse.

Covers the three Verification bullets from
``sandbox-session-manager-playground-session-reuse`` Phase 2:

1. ``test_evaluator_previews_reuses_session_with_stable_session_id`` — two
   ``evaluatorPreviews`` calls with the same ``sessionId`` reuse one backend
   session (single ``start_session`` call).
2. ``test_evaluator_previews_session_reuse_survives_rename`` — same
   ``sessionId`` but different ``name`` between calls still reuses
   (regression for Q3 key-churn).
3. ``test_stop_evaluator_session_evicts`` — ``stopEvaluatorSession`` causes
   ``backend.stop_session`` to fire for the matching key.

The tests stand up a fake `SandboxAdapter` whose `build_backend` returns a
deterministic in-memory `_FakeBackend` so we can assert against
`start_calls`/`stop_calls`/`execute_calls` directly. The wiring goes through
the real `evaluatorPreviews` mutation, real `chat_mutations.py`, real
`CodeEvaluatorRunner`, real `SandboxSessionManager` — only the backend
construction is faked.
"""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import patch

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from phoenix.db import models
from phoenix.server.sandbox import (
    _BACKEND_CACHE,
    _SANDBOX_ADAPTERS,
)
from phoenix.server.sandbox.types import (
    ExecutionResult,
    SandboxAdapter,
    SandboxBackend,
)
from phoenix.server.types import DbSessionFactory
from tests.unit.graphql import AsyncGraphQLClient

_EVALUATOR_PREVIEWS = """
mutation EvaluatorPreviews($input: EvaluatorPreviewsInput!) {
    evaluatorPreviews(input: $input) {
        results {
            evaluatorName
            error
        }
    }
}
"""

_STOP_EVALUATOR_SESSION = """
mutation StopEvaluatorSession($sessionId: String!) {
    stopEvaluatorSession(sessionId: $sessionId) {
        sessionId
        stopped
    }
}
"""


class _PermissiveConfig(BaseModel):
    model_config = ConfigDict(extra="allow")


class _FakeBackend(SandboxBackend):
    """In-memory session-capable backend with start/stop/execute counters."""

    def __init__(self) -> None:
        self.start_calls: list[str] = []
        self.stop_calls: list[str] = []
        self.execute_calls: list[tuple[str, str, Optional[int]]] = []
        self._sessions: dict[str, str] = {}

    async def start_session(self, session_key: str) -> None:
        self.start_calls.append(session_key)
        self._sessions[session_key] = "live"

    async def stop_session(self, session_key: str) -> None:
        self.stop_calls.append(session_key)
        self._sessions.pop(session_key, None)

    async def execute(
        self,
        code: str,
        session_key: str,
        timeout: Optional[int] = None,
    ) -> ExecutionResult:
        self.execute_calls.append((code, session_key, timeout))
        # The runner expects valid JSON on stdout when no error is set.
        return ExecutionResult(stdout='{"score": 1.0}', stderr="")

    async def close(self) -> None:
        self._sessions.clear()


class _FakeAdapter(SandboxAdapter):
    """SandboxAdapter that always returns the same `_FakeBackend` instance."""

    config_model = _PermissiveConfig

    def __init__(self, *, backend_type: str, backend: _FakeBackend) -> None:
        self.key = backend_type
        self.family = "WASM"
        self.display_name = f"Fake {backend_type}"
        self.language = "PYTHON"
        self.credential_specs = []
        self._backend = backend

    def build_backend(
        self,
        config: dict[str, Any],
        user_env: dict[str, str] | None = None,
    ) -> SandboxBackend:
        return self._backend


async def _seed_provider_and_config(
    db: DbSessionFactory,
    *,
    backend_type: str,
) -> int:
    """Create a SandboxProvider + SandboxConfig row for our fake backend.

    Returns the SandboxConfig database id (callers wrap with GlobalID).
    """
    async with db() as session:
        provider = models.SandboxProvider(
            backend_type=backend_type,
            language="PYTHON",
            enabled=True,
            config={},
        )
        session.add(provider)
        await session.flush()
        sandbox_config = models.SandboxConfig(
            sandbox_provider_id=provider.id,
            language="PYTHON",
            name=f"session-reuse-cfg-{backend_type.lower()}",
            config={},
            timeout=30,
        )
        session.add(sandbox_config)
        await session.flush()
        return int(sandbox_config.id)


def _purge_cache_for(backend_type: str) -> None:
    for key in [k for k in list(_BACKEND_CACHE) if k[0] == backend_type]:
        _BACKEND_CACHE.pop(key, None)


def _inline_code_evaluator_input(
    *,
    name: str,
    sandbox_config_gid: str,
    session_id: Optional[str],
) -> dict[str, Any]:
    """Build an InlineCodeEvaluatorInput payload for ``evaluatorPreviews``."""
    inline: dict[str, Any] = {
        "name": name,
        "language": "PYTHON",
        "sourceCode": "def evaluate(output):\n    return {'score': 1.0}",
        "outputConfigs": [
            {
                "continuous": {
                    "name": "score",
                    "optimizationDirection": "NONE",
                    "lowerBound": 0,
                    "upperBound": 1,
                }
            }
        ],
        "sandboxConfigId": sandbox_config_gid,
    }
    if session_id is not None:
        inline["sessionId"] = session_id
    return {
        "previews": [
            {
                "evaluator": {"inlineCodeEvaluator": inline},
                "context": {"output": "test"},
                "inputMapping": {"literalMapping": {}, "pathMapping": {}},
            }
        ]
    }


def _config_global_id(config_id: int) -> str:
    from strawberry.relay import GlobalID

    return str(GlobalID("SandboxConfig", str(config_id)))


class TestEvaluatorPreviewsSessionReuse:
    """Verification bullets 1 & 2 — manager.acquire reuses an existing session."""

    async def test_evaluator_previews_reuses_session_with_stable_session_id(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        backend_type = "SESSION_REUSE_FAKE_A"
        backend = _FakeBackend()
        adapter = _FakeAdapter(backend_type=backend_type, backend=backend)
        cfg_id = await _seed_provider_and_config(db, backend_type=backend_type)
        cfg_gid = _config_global_id(cfg_id)
        session_id = "stable-session-uuid-1111"

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                # First preview call — manager.acquire fires start_session.
                first = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="my_eval",
                            sandbox_config_gid=cfg_gid,
                            session_id=session_id,
                        )
                    },
                )
                assert first.data and not first.errors, first.errors

                # Second preview call with same sessionId — manager reuses
                # the existing session; backend.start_session NOT called
                # again.
                second = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="my_eval",
                            sandbox_config_gid=cfg_gid,
                            session_id=session_id,
                        )
                    },
                )
                assert second.data and not second.errors, second.errors

                assert backend.start_calls == [session_id], (
                    f"Expected exactly one start_session call for "
                    f"{session_id!r}, got {backend.start_calls!r}"
                )
                # Execute should have fired twice — one per preview.
                assert len(backend.execute_calls) == 2
                for _, key, _timeout in backend.execute_calls:
                    assert key == session_id, (
                        f"Expected execute keyed by {session_id!r}, got {key!r}"
                    )
        finally:
            _purge_cache_for(backend_type)

    async def test_evaluator_previews_session_reuse_survives_rename(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        """Q3 regression — renaming the inline evaluator mid-iteration must
        NOT fragment the session as long as the stable ``sessionId`` is
        unchanged.
        """
        backend_type = "SESSION_REUSE_FAKE_B"
        backend = _FakeBackend()
        adapter = _FakeAdapter(backend_type=backend_type, backend=backend)
        cfg_id = await _seed_provider_and_config(db, backend_type=backend_type)
        cfg_gid = _config_global_id(cfg_id)
        session_id = "stable-session-uuid-2222"

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                first = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="original_name",
                            sandbox_config_gid=cfg_gid,
                            session_id=session_id,
                        )
                    },
                )
                assert first.data and not first.errors, first.errors

                # Rename the evaluator (user typed a different name) but
                # keep the same sessionId — the historical bug split this
                # into two sessions; the fix routes both through the
                # manager keyed by sessionId.
                second = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="renamed_eval",
                            sandbox_config_gid=cfg_gid,
                            session_id=session_id,
                        )
                    },
                )
                assert second.data and not second.errors, second.errors

                assert backend.start_calls == [session_id], (
                    f"Rename fragmented the session: start_calls={backend.start_calls!r}"
                )
        finally:
            _purge_cache_for(backend_type)


class TestStopEvaluatorSessionMutation:
    """Verification bullet 3 — the explicit-eviction mutation stops the session."""

    async def test_stop_evaluator_session_evicts(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        backend_type = "SESSION_REUSE_FAKE_C"
        backend = _FakeBackend()
        adapter = _FakeAdapter(backend_type=backend_type, backend=backend)
        cfg_id = await _seed_provider_and_config(db, backend_type=backend_type)
        cfg_gid = _config_global_id(cfg_id)
        session_id = "session-to-evict-3333"

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                # Populate a tracked session via the same mutation path the
                # frontend uses — keeps the test honest about the wiring.
                preview = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="to_stop",
                            sandbox_config_gid=cfg_gid,
                            session_id=session_id,
                        )
                    },
                )
                assert preview.data and not preview.errors, preview.errors
                assert backend.start_calls == [session_id]
                assert backend.stop_calls == []

                # Fan-out eviction across cached backends. The matching
                # entry is idle (in_flight_count == 0 after the runner's
                # release), so the manager fires stop_session immediately.
                stop = await gql_client.execute(
                    _STOP_EVALUATOR_SESSION,
                    variables={"sessionId": session_id},
                )
                assert stop.data and not stop.errors, stop.errors
                payload = stop.data["stopEvaluatorSession"]
                assert payload["sessionId"] == session_id
                assert payload["stopped"] is True

                assert backend.stop_calls == [session_id], (
                    f"Expected stop_session to fire for {session_id!r}, "
                    f"got stop_calls={backend.stop_calls!r}"
                )
        finally:
            _purge_cache_for(backend_type)

    async def test_stop_evaluator_session_unknown_session_id_is_noop(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        """A stop for an unknown session must succeed without raising —
        the manager no-ops on absent keys and the mutation is best-effort.
        """
        # No backend setup; the eviction iterates the (possibly empty)
        # _BACKEND_CACHE and fan-outs to each. With no matching tracked
        # session, every entry is a no-op.
        result = await gql_client.execute(
            _STOP_EVALUATOR_SESSION,
            variables={"sessionId": "no-such-session-9999"},
        )
        assert result.data and not result.errors, result.errors
        payload = result.data["stopEvaluatorSession"]
        assert payload["sessionId"] == "no-such-session-9999"
        assert payload["stopped"] is True


class TestEvaluatorPreviewsBackwardCompat:
    """The optional ``sessionId`` keeps the inline path working without it."""

    async def test_evaluator_previews_without_session_id_falls_back_to_name(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        backend_type = "SESSION_REUSE_FAKE_D"
        backend = _FakeBackend()
        adapter = _FakeAdapter(backend_type=backend_type, backend=backend)
        cfg_id = await _seed_provider_and_config(db, backend_type=backend_type)
        cfg_gid = _config_global_id(cfg_id)

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                result = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="fallback_eval",
                            sandbox_config_gid=cfg_gid,
                            session_id=None,
                        )
                    },
                )
                assert result.data and not result.errors, result.errors
                # Falls back to self._name when sessionId is omitted.
                assert backend.start_calls == ["fallback_eval"]
                # Verifying we actually seeded a provider row matters for
                # the parametric isolation envelope.
                async with db() as session:
                    provider = await session.scalar(
                        select(models.SandboxProvider).where(
                            models.SandboxProvider.backend_type == backend_type
                        )
                    )
                assert provider is not None
        finally:
            _purge_cache_for(backend_type)
