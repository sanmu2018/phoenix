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
from asyncio import Event, Lock, sleep
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
    # Startup-readiness gate: the leader (is_new=True acquire) flips
    # ``start_ready`` after ``backend.start_session`` resolves so concurrent
    # followers waiting on the same key block from crossing the in_flight
    # boundary until the backend session actually exists. On failure the
    # leader records the exception in ``start_error`` and signals readiness
    # so followers wake and re-raise the same error.
    start_ready: Event = field(default_factory=Event)
    start_error: Optional[BaseException] = None


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
            # Existence + capacity + insertion are atomic under
            # ``_state_lock``: when ``_key_locks`` becomes poppable, two
            # acquires for the same logical key may hold different per-key
            # locks, so the per-key lock is no longer the authority for
            # dedup. Folding the existence check into the capacity-check +
            # insertion critical section makes ``_state_lock`` the inner
            # authority and closes the duplicate-start gap.
            tracked, is_new = await self._get_or_reserve(backend, key, session_key)
            if is_new:
                try:
                    await backend.start_session(session_key)
                except BaseException as exc:
                    # start_session failed: drop the reservation so capacity
                    # accounting stays correct, then wake any followers
                    # waiting on the readiness gate so they observe the
                    # error and re-raise. ``_key_locks`` is popped here too
                    # so a failure storm doesn't slow-leak lock entries —
                    # safe because no other coroutine can be parked on
                    # ``key_lock`` while this acquire holds it.
                    tracked.start_error = exc
                    assert self._state_lock is not None
                    async with self._state_lock:
                        self._tracked.pop(key, None)
                        self._key_locks.pop(key, None)
                    tracked.start_ready.set()
                    raise
                tracked.start_ready.set()

            # All acquires (leader and follower) wait for startup to settle
            # before crossing the in_flight boundary. The leader sets
            # ``start_ready`` immediately above; a follower may park here
            # and wake only after the leader's start_session resolves. If
            # startup failed, propagate the leader's exception so the
            # follower doesn't yield a SandboxSession against a backend
            # session that does not exist.
            await tracked.start_ready.wait()
            if tracked.start_error is not None:
                raise tracked.start_error
            tracked.in_flight_count += 1
            tracked.last_used = monotonic()

        try:
            yield SandboxSession(backend, session_key)
        finally:
            await self._release(key, key_lock)

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

    async def _get_or_reserve(
        self,
        backend: SandboxBackend,
        key: tuple[int, str],
        session_key: str,
    ) -> tuple[_TrackedSession, bool]:
        """Atomically resolve an existing tracking slot or reserve a fresh one.

        Under ``_state_lock``: (1) if ``_tracked[key]`` exists, return
        ``(existing, False)`` without rechecking capacity — followers piggy-
        back on the leader's reservation. (2) Otherwise enforce
        ``max_sessions_per_backend`` and insert a fresh ``_TrackedSession``
        whose ``start_ready`` is unset; return ``(new, True)``. The leader
        (``is_new=True``) is responsible for invoking ``backend.start_session``
        outside this lock and flipping ``start_ready`` when it resolves.

        Folding the existence check into the same critical section as the
        capacity check + insertion is what makes popping ``_key_locks`` safe:
        once the per-key lock is no longer the dedup authority, ``_state_lock``
        must be — so the existence check, capacity check, and insertion all
        run under it. The caller is responsible for popping the entry under
        ``_state_lock`` if ``backend.start_session`` fails (see ``acquire``'s
        rollback).
        """
        assert self._state_lock is not None
        async with self._state_lock:
            existing = self._tracked.get(key)
            if existing is not None:
                return existing, False
            backend_id = id(backend)
            count = sum(1 for (bid, _) in self._tracked if bid == backend_id)
            if count >= self._max_sessions_per_backend:
                raise SessionLimitExceeded()
            tracked = _TrackedSession(backend=backend, session_key=session_key)
            self._tracked[key] = tracked
            return tracked, True

    async def _release(self, key: tuple[int, str], key_lock: Lock) -> None:
        # ``acquire`` passes the lock reference it captured at acquire-time,
        # so ``_release`` never has to look ``_key_locks`` up. The lock
        # object outlives its dict entry, which is what makes popping
        # ``_key_locks`` alongside ``_tracked`` safe — a coroutine that
        # captured the lock before a concurrent eviction popped the dict
        # entry still releases through the same Lock instance.
        # Snapshot decisions under the per-key lock, run stop_session
        # outside the lock to avoid blocking other acquires on the same key.
        stop_backend: Optional[SandboxBackend] = None
        stop_key: Optional[str] = None
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
                self._key_locks.pop(key, None)
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
                    self._key_locks.pop(key, None)
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
                self._key_locks.pop(key, None)
            if stop_backend is not None and stop_key is not None:
                try:
                    await stop_backend.stop_session(stop_key)
                except Exception:
                    logger.exception(
                        "Failed to stop idle sandbox session: backend=%r key=%r",
                        type(stop_backend).__name__,
                        stop_key,
                    )
