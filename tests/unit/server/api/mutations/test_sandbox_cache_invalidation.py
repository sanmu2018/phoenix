"""Phase 3 cache-invalidation tests (task #22).

Validates that the `setSandboxCredential`, `deleteSandboxCredential`, and
`upsertOrDeleteSecrets` mutation paths correctly invalidate `_BACKEND_CACHE`
so subsequent `get_or_create_backend` calls observe rotated credentials.

Three scenarios covered:
1. Rebuild-with-new-value via setSandboxCredential — backend rebuilt with v2
   plaintext after rotation.
2. Shared-spec invalidation — VERCEL_PYTHON and VERCEL_TYPESCRIPT share
   `credential_specs` referencing VERCEL_TOKEN; rotating via
   either backend_type
   evicts BOTH cache entries.
3. upsertOrDeleteSecrets + SandboxConfig.secret_ref — rotating a user Secret
   referenced by `secret_ref` evicts the cache so the rebuilt backend sees
   the new plaintext in user_env.

Tests go through the real GraphQL mutations to exercise the wiring end-to-end.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict

import phoenix.server.sandbox as sandbox_module
from phoenix.db import models
from phoenix.server.encryption import EncryptionService
from phoenix.server.sandbox import (
    _BACKEND_CACHE,
    _SANDBOX_ADAPTERS,
    close_all_backends,
    get_or_create_backend,
)
from phoenix.server.sandbox.types import (
    ProviderCredentialSpec,
    SandboxAdapter,
    SandboxBackend,
)
from phoenix.server.types import DbSessionFactory
from tests.unit.graphql import AsyncGraphQLClient


class _PermissiveTestConfig(BaseModel):
    """Permissive config model for cache-invalidation test adapters.

    SandboxAdapter.config_model defaults to BaseModel itself, which Pydantic
    refuses to instantiate (PydanticUserError). These tests exercise cache
    eviction wiring (not config validation), so we accept any shape — env_vars
    with secret_ref, empty dicts, etc. — without forbidding extras.
    """

    model_config = ConfigDict(extra="allow")


_SET_CRED_MUTATION = """
  mutation SetSandboxCredential($input: SetSandboxCredentialInput!) {
    setSandboxCredential(input: $input) {
      backendType
      key
    }
  }
"""

_UPSERT_SECRETS_MUTATION = """
  mutation UpsertOrDeleteSecretsMutation($input: UpsertOrDeleteSecretsMutationInput!) {
    upsertOrDeleteSecrets(input: $input) {
      upsertedSecrets { key }
      deletedIds
    }
  }
"""


class _CapturingAdapter(SandboxAdapter):
    """Test adapter that records each build_backend invocation for later assertions.

    Returned backends record start_session/stop_session/close ordering via
    a shared ``call_log`` list (timestamp index → "start"/"stop"/"close")
    so eviction-coordination tests can assert that ``stop_session`` fires
    BEFORE ``close``. See ``TestSessionManagerEvictionCoordination`` below.
    """

    config_model = _PermissiveTestConfig

    def __init__(
        self,
        *,
        key: str,
        credential_specs: list[ProviderCredentialSpec],
    ) -> None:
        self.key = key
        # Any canonical family — gate doesn't filter for these tests.
        self.family = "WASM"
        self.display_name = f"Capturing {key}"
        self.language = "PYTHON"
        self.credential_specs = credential_specs
        self.received_configs: list[dict[str, Any]] = []
        self.received_user_envs: list[dict[str, str] | None] = []
        # Per-build call log shared back to the constructed backend so tests
        # can assert relative ordering across start_session, stop_session,
        # and close on a single backend instance.
        self.built_backends: list[MagicMock] = []

    def build_backend(
        self,
        config: Mapping[str, Any],
        user_env: Optional[Mapping[str, str]] = None,
    ) -> SandboxBackend:
        self.received_configs.append(dict(config))
        self.received_user_envs.append(dict(user_env) if user_env else None)
        backend = MagicMock(spec=SandboxBackend)
        call_log: list[tuple[str, str]] = []

        async def _start(session_key: str) -> None:
            call_log.append(("start", session_key))

        async def _stop(session_key: str) -> None:
            call_log.append(("stop", session_key))

        async def _close() -> None:
            call_log.append(("close", ""))

        backend.start_session = AsyncMock(side_effect=_start)
        backend.stop_session = AsyncMock(side_effect=_stop)
        backend.close = AsyncMock(side_effect=_close)
        # Stash the log on the mock so tests can read it back. ``call_log``
        # is a plain attribute on the MagicMock — not part of SandboxBackend
        # spec, so accessing it via getattr is safe.
        backend.call_log = call_log
        self.built_backends.append(backend)
        return backend


def _purge_cache_for(backend_types: list[str]) -> None:
    for backend_type in backend_types:
        for key in [k for k in list(_BACKEND_CACHE) if k[0] == backend_type]:
            _BACKEND_CACHE.pop(key, None)


class TestRebuildWithNewValue:
    """Scenario 1: setSandboxCredential rotates a Secret → backend is rebuilt with v2."""

    async def test_rebuild_after_set_sandbox_credential(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        backend_type = "REBUILD_TEST_BACKEND"
        cred_key = "REBUILD_TEST_CRED"
        adapter = _CapturingAdapter(
            key=backend_type,
            credential_specs=[
                ProviderCredentialSpec(key=cred_key, display_name="Rebuild Test Credential"),
            ],
        )
        enc = EncryptionService()

        async with db() as session:
            session.add(models.Secret(key=cred_key, value=enc.encrypt(b"v1-plaintext")))

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                # First build — populates cache with v1 plaintext.
                async with db() as session:
                    backend_v1 = await get_or_create_backend(
                        backend_type, config={}, session=session, decrypt=enc.decrypt
                    )
                assert backend_v1 is not None
                assert len(adapter.received_configs) == 1
                assert adapter.received_configs[0][cred_key] == "v1-plaintext"

                # Rotate the value in the DB so the set-credential mutation updates
                # it (insert_on_conflict). Use the mutation to fire the
                # invalidate_backend_cache_for_key hook.
                result = await gql_client.execute(
                    query=_SET_CRED_MUTATION,
                    variables={
                        "input": {
                            "backendType": backend_type,
                            "key": cred_key,
                            "value": "v2-plaintext",
                        }
                    },
                    operation_name="SetSandboxCredential",
                )
                assert not result.errors, result.errors

                # Second build — cache must have been evicted by the mutation;
                # a NEW backend is returned, reading the rotated plaintext.
                async with db() as session:
                    backend_v2 = await get_or_create_backend(
                        backend_type, config={}, session=session, decrypt=enc.decrypt
                    )
                assert backend_v2 is not None
                assert backend_v2 is not backend_v1, (
                    "Cache was not evicted: same instance returned after rotation"
                )
                assert len(adapter.received_configs) == 2
                assert adapter.received_configs[1][cred_key] == "v2-plaintext", (
                    f"Expected v2 plaintext, got {adapter.received_configs[1].get(cred_key)!r}"
                )
        finally:
            _purge_cache_for([backend_type])


class TestSharedSpecInvalidation:
    """Scenario 2: VERCEL_TOKEN is shared by VERCEL_PYTHON and VERCEL_TYPESCRIPT.

    Rotating via either backend_type must evict BOTH cache entries.
    """

    async def test_rotate_vercel_token_evicts_both_vercel_backends(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        shared_spec_key = "SHARED_VERCEL_TOKEN_TEST"
        shared_specs = [
            ProviderCredentialSpec(key=shared_spec_key, display_name="Shared Vercel Token Test"),
        ]
        py_adapter = _CapturingAdapter(key="SHARED_SPEC_PY_TEST", credential_specs=shared_specs)
        ts_adapter = _CapturingAdapter(key="SHARED_SPEC_TS_TEST", credential_specs=shared_specs)
        enc = EncryptionService()

        async with db() as session:
            session.add(models.Secret(key=shared_spec_key, value=enc.encrypt(b"initial")))

        try:
            with patch.dict(
                _SANDBOX_ADAPTERS,
                {py_adapter.key: py_adapter, ts_adapter.key: ts_adapter},
            ):
                # Populate caches for both backend_types.
                async with db() as session:
                    py_backend = await get_or_create_backend(
                        py_adapter.key, config={}, session=session, decrypt=enc.decrypt
                    )
                    ts_backend = await get_or_create_backend(
                        ts_adapter.key, config={}, session=session, decrypt=enc.decrypt
                    )
                assert py_backend is not None
                assert ts_backend is not None

                # Rotate via the PY backend_type only. The key-level fan-out
                # (invalidate_backend_cache_for_key) must evict both because
                # both adapters list `shared_spec_key` in credential_specs.
                result = await gql_client.execute(
                    query=_SET_CRED_MUTATION,
                    variables={
                        "input": {
                            "backendType": py_adapter.key,
                            "key": shared_spec_key,
                            "value": "rotated",
                        }
                    },
                    operation_name="SetSandboxCredential",
                )
                assert not result.errors, result.errors

                # Both caches must have been evicted: next call returns a new instance.
                async with db() as session:
                    py_backend_v2 = await get_or_create_backend(
                        py_adapter.key, config={}, session=session, decrypt=enc.decrypt
                    )
                    ts_backend_v2 = await get_or_create_backend(
                        ts_adapter.key, config={}, session=session, decrypt=enc.decrypt
                    )
                assert py_backend_v2 is not py_backend, (
                    "PY cache was not evicted: same instance returned after rotation"
                )
                assert ts_backend_v2 is not ts_backend, (
                    "TS cache was not evicted — shared-spec fan-out failed"
                )
        finally:
            _purge_cache_for([py_adapter.key, ts_adapter.key])


class TestUpsertOrDeleteSecretsCacheInvalidation:
    """Scenario 3: upsertOrDeleteSecrets rotates a user Secret referenced via
    SandboxConfig.env_vars[*].secret_ref → cached backend holding the v1
    plaintext must be evicted and rebuilt with v2.
    """

    async def test_rotate_user_secret_evicts_sandbox_config_cache(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        backend_type = "SECRET_REF_TEST_BACKEND"
        # User-facing env var name is DIFFERENT from the secret key to
        # validate the secret_ref indirection layer.
        env_var_name = "MY_API_KEY"
        secret_key = "USER_SECRET_ROTATE_TEST"
        enc = EncryptionService()

        # Adapter declares no credential_specs so the secret is picked up
        # purely via secret_ref hydration in user_env, not as a provider cred.
        adapter = _CapturingAdapter(key=backend_type, credential_specs=[])

        async with db() as session:
            session.add(models.Secret(key=secret_key, value=enc.encrypt(b"v1-secret")))

        # Seed a SandboxProvider + SandboxConfig whose env_vars references
        # `secret_key` by secret_ref. The upsertOrDeleteSecrets hook scans
        # SandboxConfig rows to find affected backend_types. Reuse or create
        # the PYTHON Language row — another test/seed may have already inserted it.
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
                name="secret-ref-test-config",
                config={
                    "env_vars": [
                        {
                            "kind": "secret_ref",
                            "name": env_var_name,
                            "secret_key": secret_key,
                        }
                    ]
                },
                timeout=30,
            )
            session.add(sandbox_config)
            await session.flush()

        config_with_ref: dict[str, Any] = {
            "env_vars": [
                {
                    "kind": "secret_ref",
                    "name": env_var_name,
                    "secret_key": secret_key,
                }
            ]
        }

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                # First build — populates cache with v1 plaintext in user_env.
                async with db() as session:
                    backend_v1 = await get_or_create_backend(
                        backend_type,
                        config=config_with_ref,
                        session=session,
                        decrypt=enc.decrypt,
                    )
                assert backend_v1 is not None
                assert adapter.received_user_envs[0] == {env_var_name: "v1-secret"}

                # Rotate the user Secret via the upsertOrDeleteSecrets mutation.
                # The mutation scans SandboxConfig rows and triggers
                # invalidate_backend_cache(backend_type) for matches.
                result = await gql_client.execute(
                    query=_UPSERT_SECRETS_MUTATION,
                    variables={
                        "input": {
                            "secrets": [{"key": secret_key, "value": "v2-secret"}],
                        }
                    },
                    operation_name="UpsertOrDeleteSecretsMutation",
                )
                assert not result.errors, result.errors

                # Second build — cache was invalidated, new backend sees v2.
                async with db() as session:
                    backend_v2 = await get_or_create_backend(
                        backend_type,
                        config=config_with_ref,
                        session=session,
                        decrypt=enc.decrypt,
                    )
                assert backend_v2 is not None
                assert backend_v2 is not backend_v1, (
                    "upsertOrDeleteSecrets did not evict the cache: same instance returned"
                )
                assert len(adapter.received_user_envs) == 2
                assert adapter.received_user_envs[1] == {env_var_name: "v2-secret"}, (
                    f"Expected v2-secret in user_env, got {adapter.received_user_envs[1]!r}"
                )
        finally:
            _purge_cache_for([backend_type])
            # Clean up only the rows WE added (Language may pre-exist from app seed).
            async with db() as session:
                await session.execute(
                    sa.delete(models.SandboxConfig).where(
                        models.SandboxConfig.name == "secret-ref-test-config"
                    )
                )
                await session.execute(
                    sa.delete(models.SandboxProvider).where(
                        models.SandboxProvider.backend_type == backend_type
                    )
                )
                await session.execute(
                    sa.delete(models.Secret).where(models.Secret.key == secret_key)
                )


class TestSessionManagerEvictionCoordination:
    """Phase 3 coverage: invalidation routes through the SandboxSessionManager.

    The three Verification scenarios (from work-item
    sandbox-session-manager-playground-session-reuse, phase 3):

    1. ``setSandboxCredential`` with a pre-existing tracked session causes
       the manager to ``stop_session`` BEFORE ``close()`` on the backend.
    2. ``updateSandboxConfig`` with a pre-existing tracked session evicts
       the session through the manager — regression coverage for the
       orphaned-wrapper leak Phase 3 fixes.
    3. ``close_all_backends`` drains in-flight sessions (bounded by
       ``eviction_grace_seconds``) before closing wrappers — patches the
       grace down to keep the test fast.

    Tests rely on the ``gql_client`` fixture spinning up the FastAPI
    lifespan, which calls ``register_session_manager(...)`` so the
    module-level handle (``sandbox_module._session_manager``) is wired.
    """

    async def _seed_tracked_session(
        self,
        backend: SandboxBackend,
        session_key: str,
    ) -> Any:
        """Acquire + release a session so the manager has a tracked entry."""
        manager = sandbox_module._session_manager
        assert manager is not None, (
            "Lifespan did not register a SandboxSessionManager — check gql_client fixture"
        )
        async with manager.acquire(backend, session_key):
            pass
        return manager

    async def test_credential_rotation_evicts_in_flight_session_through_manager(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        """Rotating a credential fires manager.evict_for_backend → stop_session
        runs BEFORE backend.close() on the cached wrapper."""
        backend_type = "EVICT_ROTATE_TEST_BACKEND"
        cred_key = "EVICT_ROTATE_TEST_CRED"
        adapter = _CapturingAdapter(
            key=backend_type,
            credential_specs=[
                ProviderCredentialSpec(key=cred_key, display_name="Evict Rotate Test"),
            ],
        )
        enc = EncryptionService()

        async with db() as session:
            session.add(models.Secret(key=cred_key, value=enc.encrypt(b"v1")))

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                # Build + track a session so the manager has state to evict.
                async with db() as session:
                    backend = await get_or_create_backend(
                        backend_type, config={}, session=session, decrypt=enc.decrypt
                    )
                assert backend is not None
                manager = await self._seed_tracked_session(backend, "session-1")
                assert manager is not None
                # Sanity: start_session ran exactly once.
                call_log: list[tuple[str, str]] = getattr(backend, "call_log")
                assert call_log[0] == ("start", "session-1"), call_log

                # Trigger rotation → invalidate_backend_cache_for_key →
                # invalidate_backend_cache → manager.evict_for_backend →
                # backend.stop_session (in_flight is 0 after release).
                result = await gql_client.execute(
                    query=_SET_CRED_MUTATION,
                    variables={
                        "input": {
                            "backendType": backend_type,
                            "key": cred_key,
                            "value": "v2",
                        }
                    },
                    operation_name="SetSandboxCredential",
                )
                assert not result.errors, result.errors

                # Eviction-then-close ordering on the captured backend.
                # The session is in_flight=0 at rotation time, so the manager
                # closes it synchronously inside _evict_matching.
                kinds = [k for k, _ in call_log]
                assert "stop" in kinds, f"stop_session never ran: {call_log}"
                assert "close" in kinds, f"close never ran: {call_log}"
                assert kinds.index("stop") < kinds.index("close"), (
                    f"stop_session must run BEFORE close: {call_log}"
                )
        finally:
            _purge_cache_for([backend_type])

    async def test_update_sandbox_config_evicts_session_through_manager(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        """Updating a SandboxConfig invalidates the cached backend and evicts
        any tracked session through the manager BEFORE close. Regression
        coverage for the orphaned-wrapper leak (Phase 3 D5)."""
        from strawberry.relay import GlobalID

        from phoenix.server.api.types.SandboxConfig import SandboxConfig as SandboxConfigGQL

        backend_type = "EVICT_UPDATE_TEST_BACKEND"
        adapter = _CapturingAdapter(key=backend_type, credential_specs=[])

        async with db() as session:
            provider = models.SandboxProvider(
                backend_type=backend_type,
                language="PYTHON",
                enabled=True,
            )
            session.add(provider)
            await session.flush()
            cfg = models.SandboxConfig(
                sandbox_provider_id=provider.id,
                language="PYTHON",
                name="evict-update-test-config",
                config={},
                timeout=30,
            )
            session.add(cfg)
            await session.flush()
            cfg_global_id = str(GlobalID(SandboxConfigGQL.__name__, str(cfg.id)))

        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                async with db() as session:
                    backend = await get_or_create_backend(
                        backend_type,
                        config={},
                        session=session,
                        decrypt=EncryptionService().decrypt,
                    )
                assert backend is not None
                await self._seed_tracked_session(backend, "session-1")
                call_log: list[tuple[str, str]] = getattr(backend, "call_log")
                assert ("start", "session-1") in call_log

                # Update the SandboxConfig — Phase 3 wires this mutation to
                # invalidate_backend_cache(provider.backend_type).
                result = await gql_client.execute(
                    query=(
                        "mutation U($input: UpdateSandboxConfigInput!) { "
                        "updateSandboxConfig(input: $input) { "
                        "sandboxConfig { id name } } }"
                    ),
                    variables={
                        "input": {
                            "id": cfg_global_id,
                            "timeout": 60,
                        }
                    },
                    operation_name="U",
                )
                assert not result.errors, result.errors

                kinds = [k for k, _ in call_log]
                assert "stop" in kinds and "close" in kinds, (
                    f"updateSandboxConfig did not fan out through the manager: {call_log}"
                )
                assert kinds.index("stop") < kinds.index("close"), (
                    f"stop_session must run BEFORE close: {call_log}"
                )
        finally:
            _purge_cache_for([backend_type])
            async with db() as session:
                await session.execute(
                    sa.delete(models.SandboxConfig).where(
                        models.SandboxConfig.name == "evict-update-test-config"
                    )
                )
                await session.execute(
                    sa.delete(models.SandboxProvider).where(
                        models.SandboxProvider.backend_type == backend_type
                    )
                )

    async def test_close_all_backends_drains_in_flight_then_closes(
        self,
        db: DbSessionFactory,
        gql_client: AsyncGraphQLClient,
    ) -> None:
        """``close_all_backends`` waits up to ``eviction_grace_seconds`` for
        in-flight sessions to drain; if the window expires they stay
        marked-for-eviction and the wrapper is still closed.

        Patches the grace down to ~0.1s so the test resolves quickly. An
        Event-gated fake ``execute`` holds the session in-flight until the
        test releases it, so timing is deterministic — no sleep loops.
        """
        import asyncio

        backend_type = "EVICT_SHUTDOWN_TEST_BACKEND"
        adapter = _CapturingAdapter(key=backend_type, credential_specs=[])
        manager = sandbox_module._session_manager
        assert manager is not None
        original_grace = manager.eviction_grace_seconds
        manager.eviction_grace_seconds = 0.1
        try:
            with patch.dict(_SANDBOX_ADAPTERS, {backend_type: adapter}):
                async with db() as session:
                    backend = await get_or_create_backend(
                        backend_type,
                        config={},
                        session=session,
                        decrypt=EncryptionService().decrypt,
                    )
                assert backend is not None

                # Acquire a session, hold it in_flight, drive close_all_backends
                # concurrently, then release the session. The manager's grace
                # window is 0.1s — by the time we release ~0.2s later, the
                # backend is marked-for-eviction; release fires stop_session.
                release_event = asyncio.Event()

                async def hold_session() -> None:
                    async with manager.acquire(backend, "session-1"):
                        await release_event.wait()

                hold_task = asyncio.create_task(hold_session())
                # Yield so the acquire actually progresses to in_flight > 0.
                await asyncio.sleep(0)
                await asyncio.sleep(0.01)

                # Drive shutdown — must not deadlock waiting for in_flight.
                close_task = asyncio.create_task(close_all_backends())
                # Give close_all_backends time to exhaust its grace window.
                await asyncio.sleep(0.25)
                # Release the session so the manager's _release fires
                # stop_session (the entry is marked_for_eviction).
                release_event.set()
                await hold_task
                await close_task

                call_log: list[tuple[str, str]] = getattr(backend, "call_log")
                kinds = [k for k, _ in call_log]
                assert "start" in kinds
                assert "close" in kinds, f"close never ran: {call_log}"
                # stop_session fires from the release path (mark-for-eviction
                # promoted on the in_flight=0 transition).
                assert "stop" in kinds, (
                    f"mark-for-eviction did not fire stop on release: {call_log}"
                )
        finally:
            manager.eviction_grace_seconds = original_grace
            _purge_cache_for([backend_type])
