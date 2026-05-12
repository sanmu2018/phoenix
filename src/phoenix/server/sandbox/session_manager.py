"""
SandboxSessionManager — central authority for sandbox session lifecycle.

Hoists per-key locking above the backend layer so all session-capable
backends (Modal, Vercel, E2B, Daytona) share one creation-dedup pathway.
Tracks idle TTL and in-flight reference counts; evicts unused sessions
in a background sweeper. Stateless backends (``BaseNoSessionBackend``)
are short-circuited — ``acquire`` returns a handle whose ``execute``
calls the backend directly without locking or state tracking.

The manager is constructed in ``create_app`` and registered in
``_lifespan``'s ``AsyncExitStack`` alongside other daemons (one per
process). Application code obtains the instance via
``info.context.sandbox_session_manager`` (GraphQL) or
``request.app.state.sandbox_session_manager`` (FastAPI) — never by
constructing a new manager.
"""

from __future__ import annotations

import logging
import os
from asyncio import Lock, sleep
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import AsyncIterator, Callable, Optional

from phoenix.server.sandbox.types import (
    BaseNoSessionBackend,
    ExecutionResult,
    SandboxBackend,
)
from phoenix.server.types import DaemonTask

logger = logging.getLogger(__name__)


_DEFAULT_IDLE_TTL_SECONDS = 300.0
_DEFAULT_SWEEP_INTERVAL_SECONDS = 30.0
_DEFAULT_EVICTION_GRACE_SECONDS = 5.0
_DEFAULT_MAX_SESSIONS_PER_BACKEND = 32


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r; using default %s", name, raw, default)
        return default


class SessionLimitExceeded(Exception):
    """Raised by ``SandboxSessionManager.acquire`` when a backend has reached
    ``max_sessions_per_backend``.

    Capacity refusal — not a backend execution failure. Raised BEFORE
    ``backend.start_session`` is called, and the manager's internal state
    is unchanged when the exception propagates. ``CodeEvaluatorRunner``
    converts this to an ``ExecutionResult`` with a stable error code so
    the UI sees a deterministic, user-actionable message.
    """

    MESSAGE = "session_limit_exceeded"

    def __init__(self, message: str = MESSAGE) -> None:
        super().__init__(message)


@dataclass
class _TrackedSession:
    backend: SandboxBackend
    session_key: str
    in_flight_count: int = 0
    last_used: float = field(default_factory=monotonic)
    marked_for_eviction: bool = False


class SandboxSession:
    """Handle returned by ``SandboxSessionManager.acquire``.

    Calls ``backend.execute(code, session_key, timeout=timeout)`` directly;
    in-flight ref-counting and last-used tracking are handled by the manager
    around the ``async with`` boundary.
    """

    def __init__(
        self,
        backend: SandboxBackend,
        session_key: str,
    ) -> None:
        self._backend = backend
        self._session_key = session_key

    @property
    def session_key(self) -> str:
        return self._session_key

    async def execute(
        self,
        code: str,
        timeout: Optional[int] = None,
    ) -> ExecutionResult:
        return await self._backend.execute(code, self._session_key, timeout=timeout)


class SandboxSessionManager(DaemonTask):
    """Central session lifecycle authority for sandbox backends.

    - ``acquire(backend, session_key)`` — async context manager. Starts (or
      reuses) a session and yields a ``SandboxSession``. Enforces
      ``max_sessions_per_backend`` before invoking ``backend.start_session``.
    - ``evict_for_backend(backend)`` / ``evict_for_backend_type(type)`` /
      ``evict_for_backend_key(backend, key)`` — invalidation API. Stops
      backend sessions whose in-flight count is zero immediately; marks
      in-use sessions for stop-on-release.
    - Background sweeper (``_run``) evicts entries whose ``last_used``
      crosses the idle TTL with ``in_flight_count == 0``.

    Stateless backends (``BaseNoSessionBackend``) bypass all locking and
    state tracking — ``acquire`` returns a handle wired directly to
    ``backend.execute``.
    """

    def __init__(
        self,
        *,
        idle_ttl_seconds: Optional[float] = None,
        sweep_interval_seconds: float = _DEFAULT_SWEEP_INTERVAL_SECONDS,
        eviction_grace_seconds: float = _DEFAULT_EVICTION_GRACE_SECONDS,
        max_sessions_per_backend: int = _DEFAULT_MAX_SESSIONS_PER_BACKEND,
    ) -> None:
        super().__init__()
        self._idle_ttl_seconds: float = (
            idle_ttl_seconds
            if idle_ttl_seconds is not None
            else _env_float(
                "PHOENIX_SANDBOX_SESSION_IDLE_TTL_SECONDS",
                _DEFAULT_IDLE_TTL_SECONDS,
            )
        )
        self._sweep_interval_seconds = sweep_interval_seconds
        self._eviction_grace_seconds = eviction_grace_seconds
        self._max_sessions_per_backend = max_sessions_per_backend

        # asyncio primitives are loop-bound; construct lazily inside the
        # running loop (start() / acquire()) rather than at __init__ time.
        self._state_lock: Optional[Lock] = None
        self._key_locks: dict[tuple[int, str], Lock] = {}
        self._tracked: dict[tuple[int, str], _TrackedSession] = {}

    @property
    def eviction_grace_seconds(self) -> float:
        """Maximum time ``evict_for_backend*`` waits for in-flight sessions to
        drain before returning. Settable to compress shutdown / invalidation
        timing in tests."""
        return self._eviction_grace_seconds

    @eviction_grace_seconds.setter
    def eviction_grace_seconds(self, value: float) -> None:
        self._eviction_grace_seconds = value

    # ------------------------------------------------------------------
    # Public surface
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(
        self,
        backend: SandboxBackend,
        session_key: str,
    ) -> AsyncIterator[SandboxSession]:
        """Yield a ``SandboxSession`` bound to (backend, session_key).

        For session-capable backends: ensures the session is started under
        a per-key lock, increments in-flight count on enter, decrements on
        exit, and fires ``backend.stop_session`` on exit if the session was
        marked for eviction and in-flight reaches zero.

        For ``BaseNoSessionBackend`` backends: short-circuits — no locking,
        no state tracking, no eviction.
        """
        if isinstance(backend, BaseNoSessionBackend):
            yield SandboxSession(backend, session_key)
            return

        key = (id(backend), session_key)
        await self._ensure_state_lock()
        key_lock = await self._get_or_create_key_lock(key)

        async with key_lock:
            tracked = self._tracked.get(key)
            if tracked is None:
                await self._enforce_capacity_or_raise(backend)
                await backend.start_session(session_key)
                tracked = _TrackedSession(backend=backend, session_key=session_key)
                self._tracked[key] = tracked
            tracked.in_flight_count += 1
            tracked.last_used = monotonic()

        try:
            yield SandboxSession(backend, session_key)
        finally:
            await self._release(key)

    async def evict_for_backend(self, backend: SandboxBackend) -> None:
        """Evict all tracked sessions for ``backend``.

        Sessions with ``in_flight_count == 0`` are stopped immediately.
        In-use sessions are marked for eviction and stopped on release.
        Stateless backends are a no-op.
        """
        if isinstance(backend, BaseNoSessionBackend):
            return
        await self._evict_matching(lambda t: t.backend is backend)

    async def evict_for_backend_type(self, backend_type: type) -> None:
        """Evict all tracked sessions for any backend that is an instance of
        ``backend_type``."""
        await self._evict_matching(lambda t: isinstance(t.backend, backend_type))

    async def evict_for_backend_key(
        self,
        backend: SandboxBackend,
        session_key: str,
    ) -> None:
        """Evict a single (backend, session_key) entry. No-op if untracked
        or if backend is stateless."""
        if isinstance(backend, BaseNoSessionBackend):
            return
        target_key = (id(backend), session_key)

        def _match(t: _TrackedSession) -> bool:
            return (id(t.backend), t.session_key) == target_key

        await self._evict_matching(_match)

    # ------------------------------------------------------------------
    # DaemonTask body
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        await self._ensure_state_lock()
        while self._running:
            try:
                await self._sweep_idle()
            except Exception:
                logger.exception("Sandbox session manager sweep failed")
            await sleep(self._sweep_interval_seconds)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _ensure_state_lock(self) -> None:
        if self._state_lock is None:
            self._state_lock = Lock()

    async def _get_or_create_key_lock(self, key: tuple[int, str]) -> Lock:
        assert self._state_lock is not None
        async with self._state_lock:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = Lock()
                self._key_locks[key] = lock
            return lock

    async def _enforce_capacity_or_raise(self, backend: SandboxBackend) -> None:
        # Capacity is per-backend. Count tracked entries for this backend
        # (matched by identity) — must run BEFORE backend.start_session so
        # rejection leaves manager state unchanged.
        backend_id = id(backend)
        count = sum(1 for (bid, _) in self._tracked if bid == backend_id)
        if count >= self._max_sessions_per_backend:
            raise SessionLimitExceeded()

    async def _release(self, key: tuple[int, str]) -> None:
        # Snapshot decisions under the per-key lock, run stop_session
        # outside the lock to avoid blocking other acquires on the same key.
        stop_backend: Optional[SandboxBackend] = None
        stop_key: Optional[str] = None
        key_lock = self._key_locks.get(key)
        if key_lock is None:
            return
        async with key_lock:
            tracked = self._tracked.get(key)
            if tracked is None:
                return
            tracked.in_flight_count = max(0, tracked.in_flight_count - 1)
            tracked.last_used = monotonic()
            if tracked.marked_for_eviction and tracked.in_flight_count == 0:
                stop_backend = tracked.backend
                stop_key = tracked.session_key
                self._tracked.pop(key, None)
        if stop_backend is not None and stop_key is not None:
            try:
                await stop_backend.stop_session(stop_key)
            except Exception:
                logger.exception(
                    "Failed to stop sandbox session on release: backend=%r key=%r",
                    type(stop_backend).__name__,
                    stop_key,
                )

    async def _evict_matching(
        self,
        predicate: Callable[[_TrackedSession], bool],
    ) -> None:
        await self._ensure_state_lock()
        # Collect targets under the state lock; act on each entry under its
        # per-key lock so we don't race with concurrent acquire().
        assert self._state_lock is not None
        async with self._state_lock:
            targets = [k for k, t in list(self._tracked.items()) if predicate(t)]
        # First pass: stop idle sessions immediately; mark in-flight sessions
        # for eviction. Track the latter so we can wait for them to drain.
        marked: list[tuple[int, str]] = []
        for key in targets:
            key_lock = self._key_locks.get(key)
            if key_lock is None:
                continue
            stop_backend: Optional[SandboxBackend] = None
            stop_key: Optional[str] = None
            async with key_lock:
                tracked = self._tracked.get(key)
                if tracked is None:
                    continue
                if tracked.in_flight_count == 0:
                    stop_backend = tracked.backend
                    stop_key = tracked.session_key
                    self._tracked.pop(key, None)
                else:
                    tracked.marked_for_eviction = True
                    marked.append(key)
            if stop_backend is not None and stop_key is not None:
                try:
                    await stop_backend.stop_session(stop_key)
                except Exception:
                    logger.exception(
                        "Failed to stop sandbox session during eviction: backend=%r key=%r",
                        type(stop_backend).__name__,
                        stop_key,
                    )
        # Second pass: wait up to ``eviction_grace_seconds`` for marked
        # sessions to drain. Once ``in_flight_count`` reaches zero, the
        # corresponding ``_release`` callback fires ``backend.stop_session``
        # and removes the entry from ``_tracked``. If the grace window
        # expires with sessions still in-flight, leave them marked — the
        # next release will still close them. This is a bounded best-effort
        # drain, not a guaranteed barrier.
        if not marked:
            return
        deadline = monotonic() + self._eviction_grace_seconds
        poll_interval = min(0.05, max(0.005, self._eviction_grace_seconds / 20.0))
        while marked and monotonic() < deadline:
            await sleep(poll_interval)
            async with self._state_lock:
                marked = [k for k in marked if k in self._tracked]

    async def _sweep_idle(self) -> None:
        await self._ensure_state_lock()
        now = monotonic()
        ttl = self._idle_ttl_seconds
        assert self._state_lock is not None
        async with self._state_lock:
            candidates = [
                k
                for k, t in list(self._tracked.items())
                if t.in_flight_count == 0 and now - t.last_used > ttl
            ]
        for key in candidates:
            key_lock = self._key_locks.get(key)
            if key_lock is None:
                continue
            stop_backend: Optional[SandboxBackend] = None
            stop_key: Optional[str] = None
            async with key_lock:
                tracked = self._tracked.get(key)
                if tracked is None:
                    continue
                if tracked.in_flight_count != 0:
                    continue
                if monotonic() - tracked.last_used <= ttl:
                    continue
                stop_backend = tracked.backend
                stop_key = tracked.session_key
                self._tracked.pop(key, None)
            if stop_backend is not None and stop_key is not None:
                try:
                    await stop_backend.stop_session(stop_key)
                except Exception:
                    logger.exception(
                        "Failed to stop idle sandbox session: backend=%r key=%r",
                        type(stop_backend).__name__,
                        stop_key,
                    )
