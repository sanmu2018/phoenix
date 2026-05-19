"""Phase 1 caller-wiring integration tests for sandbox session reuse under
the cross-replica-deterministic-sandbox-session-reuse refactor.

Each request now builds a fresh ``SandboxBackend`` via
``build_sandbox_backend`` (no process-level backend cache), and session
reuse is bound at the provider via
``backend.find_or_create_session(session_key)``. Two ``evaluatorPreviews``
calls that derive the same ``session_key`` ("inline:<user_id>:<config_gid>:<language>"
for inline previews, "evaluator:<id>" for stored evaluators) must therefore:

1. Construct two distinct backend wrapper instances (fresh ``build_backend``
   per request) but converge on a single remote sandbox handle via a
   provider-native list/get (mirrored here by a module-level fake map).
2. Re-use the manager-tracked session entry, so the second call does not
   re-invoke ``find_or_create_session`` on the same key.

The inline-preview session key is now derived server-side from the caller's
auth identity and ``(sandbox_config_id, language)``. The frontend no longer
mints or echoes a session id, and the previously load-bearing test path —
``stopEvaluatorSession`` — has been removed: eviction is the manager's idle
TTL.

The fake adapter below builds a fresh ``_FakeBackend`` instance per
``build_backend`` call (so wrapper identity churns the way Phoenix's real
factory does) but routes every session through a module-level
``_FAKE_PROVIDER_SESSIONS`` map keyed on ``session_key``. That mirrors how
Modal/E2B/Daytona converge cross-wrapper via name/metadata/label and how
Vercel converges in-process via ``_session_id_map``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional
from unittest.mock import patch

from pydantic import BaseModel, ConfigDict

from phoenix.db import models
from phoenix.server.sandbox import _SANDBOX_ADAPTERS
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


class _PermissiveConfig(BaseModel):
    model_config = ConfigDict(extra="allow")


# Module-level provider-side state shared across every fake backend wrapper.
# Mirrors the cross-wrapper convergence guarantee from Modal (per-app name
# namespace), E2B (metadata list), Daytona (label list), and Vercel
# (in-process ``_session_id_map``): two fresh wrapper instances asking
# ``find_or_create_session(key)`` for the same key get the same opaque
# remote handle, and a ``close_session(key)`` from any wrapper releases it.
_FAKE_PROVIDER_SESSIONS: dict[str, object] = {}
_FAKE_FIND_CALLS: list[tuple[int, str]] = []
_FAKE_CLOSE_CALLS: list[tuple[int, str]] = []
_FAKE_EXECUTE_CALLS: list[tuple[int, str, str]] = []


def _reset_fake_provider_state() -> None:
    _FAKE_PROVIDER_SESSIONS.clear()
    _FAKE_FIND_CALLS.clear()
    _FAKE_CLOSE_CALLS.clear()
    _FAKE_EXECUTE_CALLS.clear()


class _FakeRemoteHandle:
    """Opaque per-key handle the fake adapter hands back to callers."""

    def __init__(self, session_key: str) -> None:
        self.session_key = session_key


class _FakeBackend(SandboxBackend):
    """In-memory session-capable fake routed through a module-level map.

    Each ``_FakeAdapter.build_backend`` call constructs a fresh instance —
    mirroring the real factory's no-cache shape — but ``find_or_create_session``
    looks up / installs into ``_FAKE_PROVIDER_SESSIONS`` so the second
    fresh wrapper for the same ``session_key`` returns the existing remote
    handle. ``id(self)`` is recorded on each call so the test can assert
    that two distinct wrappers really did participate.
    """

    def __init__(self) -> None:
        self.secret_values = frozenset()

    def config_fingerprint(self) -> str:
        # Stable across fresh wrappers because the fake config carries no
        # runtime-affecting fields; the manager composes this into
        # f"{session_key}#{fingerprint}" for its internal keying.
        return "fake"

    async def find_or_create_session(self, session_key: str) -> object:
        _FAKE_FIND_CALLS.append((id(self), session_key))
        handle = _FAKE_PROVIDER_SESSIONS.get(session_key)
        if handle is None:
            handle = _FakeRemoteHandle(session_key)
            _FAKE_PROVIDER_SESSIONS[session_key] = handle
        return handle

    async def execute_in_session(
        self,
        handle: object,
        code: str,
        timeout: Optional[int] = None,
    ) -> ExecutionResult:
        assert isinstance(handle, _FakeRemoteHandle)
        _FAKE_EXECUTE_CALLS.append((id(self), handle.session_key, code))
        return ExecutionResult(stdout='{"score": 1.0}', stderr="")

    async def close_session(self, session_key: str) -> None:
        _FAKE_CLOSE_CALLS.append((id(self), session_key))
        _FAKE_PROVIDER_SESSIONS.pop(session_key, None)

    async def execute(
        self,
        code: str,
        session_key: str,
        timeout: Optional[int] = None,
    ) -> ExecutionResult:
        # Direct one-shot path; not exercised by these manager-mediated tests
        # but required by the ABC.
        return ExecutionResult(stdout='{"score": 1.0}', stderr="")

    async def close(self) -> None:
        return None


class _FakeAdapter(SandboxAdapter):
    """SandboxAdapter that builds a FRESH _FakeBackend per build_backend call.

    Mirrors the no-cache shape of the real ``build_sandbox_backend``: each
    request constructs a new wrapper. Convergence on the remote sandbox is
    bound provider-side (here, the module-level ``_FAKE_PROVIDER_SESSIONS``).
    """

    config_model = _PermissiveConfig

    def __init__(self, *, backend_type: str) -> None:
        self.key = backend_type
        self.family = "WASM"
        self.display_name = f"Fake {backend_type}"
        self.language = "PYTHON"
        self.credential_specs = []
        self.built_backends: list[_FakeBackend] = []

    def build_backend(
        self,
        config: Mapping[str, Any],
        user_env: Optional[Mapping[str, str]] = None,
    ) -> SandboxBackend:
        backend = _FakeBackend()
        self.built_backends.append(backend)
        return backend


async def _seed_provider_and_config(
    db: DbSessionFactory,
    *,
    backend_type: str,
) -> int:
    async with db() as session:
        provider = models.SandboxProvider(
            backend_type=backend_type,
            language="PYTHON",
            enabled=True,
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


def _inline_code_evaluator_input(
    *,
    name: str,
    sandbox_config_gid: str,
) -> dict[str, Any]:
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


def _expected_composed_key(*, cfg_gid: str, language: str = "PYTHON") -> str:
    """Mirrors the server-side inline-session key derivation from
    ``chat_mutations.py`` plus the manager's ``_composite_key`` suffix.

    The mutation derives ``f"inline:{user_id or 'anon'}:{config_gid}:{language}"``
    from the caller's auth context and the request's ``(config, language)``.
    Tests run through ``gql_client`` unauthenticated, so ``user_id`` is None
    and the sentinel ``"anon"`` is used. The manager then appends
    ``"#" + backend.config_fingerprint()`` (here ``"#fake"``) for its
    internal ``_tracked`` map and propagates the composite to
    ``find_or_create_session`` / ``close_session``.
    """
    return f"inline:anon:{cfg_gid}:{language}#fake"


class TestEvaluatorPreviewsSessionReuseConvergesAcrossWrappers:
    """Two ``evaluatorPreviews`` calls deriving the same inline session key
    converge on a single remote sandbox handle even though every request
    builds a fresh backend wrapper.
    """

    async def test_two_sequential_previews_share_one_remote_handle(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        _reset_fake_provider_state()
        backend_type = "SESSION_REUSE_FAKE_A"
        adapter = _FakeAdapter(backend_type=backend_type)
        cfg_id = await _seed_provider_and_config(db, backend_type=backend_type)
        cfg_gid = _config_global_id(cfg_id)
        composed_key = _expected_composed_key(cfg_gid=cfg_gid)

        with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
            first = await gql_client.execute(
                _EVALUATOR_PREVIEWS,
                variables={
                    "input": _inline_code_evaluator_input(
                        name="my_eval",
                        sandbox_config_gid=cfg_gid,
                    )
                },
            )
            assert first.data and not first.errors, first.errors

            second = await gql_client.execute(
                _EVALUATOR_PREVIEWS,
                variables={
                    "input": _inline_code_evaluator_input(
                        name="my_eval",
                        sandbox_config_gid=cfg_gid,
                    )
                },
            )
            assert second.data and not second.errors, second.errors

        # No-cache shape: each request constructs a fresh wrapper.
        assert len(adapter.built_backends) == 2
        assert adapter.built_backends[0] is not adapter.built_backends[1]

        # Manager dedupes via _tracked: only the FIRST request's wrapper
        # gets to call find_or_create_session. The second request finds
        # the tracked entry under the same session_key and reuses the
        # already-bound remote handle.
        assert len(_FAKE_FIND_CALLS) == 1, (
            f"Expected exactly one find_or_create_session call; got {_FAKE_FIND_CALLS!r}"
        )
        assert _FAKE_FIND_CALLS[0][1] == composed_key
        assert composed_key in _FAKE_PROVIDER_SESSIONS

        # Both wrappers executed against the SAME remote handle: the
        # manager only invokes find_or_create_session on the leader, but
        # each request yields a SandboxSession bound to its own (fresh)
        # wrapper instance — the wrapper that routes execute_in_session
        # is the request's own wrapper, using the leader's bound handle.
        # The handle is what's load-bearing for convergence.
        assert len(_FAKE_EXECUTE_CALLS) == 2
        for _, key, _code in _FAKE_EXECUTE_CALLS:
            assert key == composed_key
        # Provider state holds exactly ONE handle for this key, so both
        # wrappers necessarily routed through the same remote sandbox.
        assert composed_key in _FAKE_PROVIDER_SESSIONS

    async def test_rename_does_not_fragment_session(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        """Q3 regression — a rename mid-iteration must not fragment the
        session. Under the server-derived key, ``name`` is not part of the
        derivation at all (the key is ``(user_id, config_gid, language)``),
        so two renames produce the same session_key by construction.
        """
        _reset_fake_provider_state()
        backend_type = "SESSION_REUSE_FAKE_B"
        adapter = _FakeAdapter(backend_type=backend_type)
        cfg_id = await _seed_provider_and_config(db, backend_type=backend_type)
        cfg_gid = _config_global_id(cfg_id)
        composed_key = _expected_composed_key(cfg_gid=cfg_gid)

        with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
            first = await gql_client.execute(
                _EVALUATOR_PREVIEWS,
                variables={
                    "input": _inline_code_evaluator_input(
                        name="original_name",
                        sandbox_config_gid=cfg_gid,
                    )
                },
            )
            assert first.data and not first.errors, first.errors

            second = await gql_client.execute(
                _EVALUATOR_PREVIEWS,
                variables={
                    "input": _inline_code_evaluator_input(
                        name="renamed_eval",
                        sandbox_config_gid=cfg_gid,
                    )
                },
            )
            assert second.data and not second.errors, second.errors

        # Rename did not produce a second find_or_create_session — both
        # requests routed to the same composed session_key.
        assert len(_FAKE_FIND_CALLS) == 1, (
            f"Rename fragmented the session: find_calls={_FAKE_FIND_CALLS!r}"
        )
        assert _FAKE_FIND_CALLS[0][1] == composed_key


class TestInlineSessionKeyPartitioning:
    """The inline session key partitions on ``(user_id, config_gid, language)``.
    Same triple → same session (reuse). Different triple → distinct sessions.

    The reuse-on-same-triple case is covered by
    ``TestEvaluatorPreviewsSessionReuseConvergesAcrossWrappers``. The
    distinct-on-different-config case below guards the partitioning
    invariant: two different sandbox configs must never share a session
    even within a single caller's session graph.
    """

    async def test_distinct_configs_produce_distinct_sessions(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        _reset_fake_provider_state()
        backend_type = "SESSION_REUSE_FAKE_D"
        adapter = _FakeAdapter(backend_type=backend_type)
        # One provider, two configs against it — the SandboxProvider table's
        # UNIQUE (backend_type, language) constraint forbids two providers
        # with the same backend_type, but multiple configs under one provider
        # is exactly the multi-config-per-backend case we want to partition.
        cfg_id_a = await _seed_provider_and_config(db, backend_type=backend_type)
        async with db() as session:
            existing = await session.get(models.SandboxConfig, cfg_id_a)
            assert existing is not None
            cfg_b = models.SandboxConfig(
                sandbox_provider_id=existing.sandbox_provider_id,
                language="PYTHON",
                name=f"session-reuse-cfg-{backend_type.lower()}-b",
                config={},
                timeout=30,
            )
            session.add(cfg_b)
            await session.flush()
            cfg_id_b = int(cfg_b.id)
        assert cfg_id_a != cfg_id_b
        cfg_gid_a = _config_global_id(cfg_id_a)
        cfg_gid_b = _config_global_id(cfg_id_b)
        composed_a = _expected_composed_key(cfg_gid=cfg_gid_a)
        composed_b = _expected_composed_key(cfg_gid=cfg_gid_b)
        assert composed_a != composed_b

        with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
            for cfg_gid in (cfg_gid_a, cfg_gid_b):
                r = await gql_client.execute(
                    _EVALUATOR_PREVIEWS,
                    variables={
                        "input": _inline_code_evaluator_input(
                            name="ev",
                            sandbox_config_gid=cfg_gid,
                        )
                    },
                )
                assert r.data and not r.errors, r.errors

        # Distinct configs → distinct tracked entries → two find_or_create_session
        # calls, one per derived key.
        assert composed_a in _FAKE_PROVIDER_SESSIONS
        assert composed_b in _FAKE_PROVIDER_SESSIONS
        find_keys = sorted(k for _, k in _FAKE_FIND_CALLS)
        assert find_keys == sorted([composed_a, composed_b]), (
            f"Expected one find per derived key; got {_FAKE_FIND_CALLS!r}"
        )
