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

import asyncio
import logging
import os
from asyncio import Event, Lock, Task, sleep
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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid int for %s=%r; using default %s", name, raw, default)
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


class SessionInvalidated(Exception):
    """Raised by ``SandboxSessionManager.acquire`` when a tracked session has
    been marked for eviction.

    Admit-block refusal — the existing entry is draining and will be stopped
    once in-flight users release. New admits must not extend its lifetime
    past the explicit stop request. Raised BEFORE ``backend.start_session``
    is called; the manager's internal state is unchanged when the exception
    propagates. ``CodeEvaluatorRunner`` converts this to an
    ``ExecutionResult`` with a stable error code so the UI sees a
    deterministic, user-actionable message rather than a silent restart.
    """

    MESSAGE = "session_invalidated"

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
      Refuses admits onto entries marked for eviction by raising
      ``SessionInvalidated``.
    - ``evict_for_backend(backend)`` / ``evict_for_backend_type(type)`` /
      ``evict_for_backend_key(backend, key)`` — invalidation API. Stops
      backend sessions whose in-flight count is zero immediately; marks
      in-use sessions for stop-on-release.
    - ``schedule_eviction(backend, session_key)`` — fire-and-forget API for
      callers that cannot ``await`` the eviction inline (notably the
      per-execute timeout teardown in ``CodeEvaluatorRunner``, which runs
      in both GraphQL request and ``ExperimentRunner`` daemon scopes). The
      created task is retained on the manager so shutdown awaits it.
    - Background sweeper (``_run``) evicts entries whose ``last_used``
      crosses the idle TTL with ``in_flight_count == 0``.

    Stateless backends (``BaseNoSessionBackend``) bypass all locking and
    state tracking — ``acquire`` returns a handle wired directly to
    ``backend.execute``.

    **Lock-order invariant.** Two locks coexist: a single ``_state_lock``
    that guards the ``_tracked`` / ``_key_locks`` dicts, and one
    ``asyncio.Lock`` per ``(backend_id, session_key)`` in ``_key_locks``.
    ``acquire`` takes the per-key lock OUTER and ``_state_lock`` INNER
    (existence check, capacity check, and reservation insertion all run
    under ``_state_lock`` so a fresh same-key acquire sees a leader's
    reservation immediately). ``evict_for_backend*`` and ``_sweep_idle``
    take ``_state_lock`` only long enough to snapshot the target keys,
    then release it before acquiring each per-key lock — holding both
    locks across an ``await`` on ``backend.stop_session`` would deadlock
    against an in-flight acquire on the same key.

    **Shutdown ordering.** ``stop()`` (overridden from ``DaemonTask``)
    drains in three phases under the inherited 10s ceiling:
    (1) await ``_pending_tasks`` (fire-and-forget evictions from
    ``schedule_eviction``) under ``wait_for(eviction_grace_seconds)`` so
    their underlying ``stop_session`` calls complete rather than being
    cancelled mid-flight; (2) snapshot unique backends from ``_tracked``
    and call ``evict_for_backend`` on each (marks in-flight entries and
    waits up to ``eviction_grace_seconds`` for them to drain);
    (3) ``super().stop()`` cancels the sweeper. ``_pending_tasks`` is a
    set held adjacent to (not merged into) the inherited ``self._tasks``:
    ``self._tasks`` holds tasks spawned from ``_run`` and is drained by
    cancel-then-gather; ``_pending_tasks`` holds externally-scheduled
    work the manager owns through completion, so it is awaited (not
    cancelled) at shutdown.
    """

    def __init__(
        self,
        *,
        idle_ttl_seconds: Optional[float] = None,
        sweep_interval_seconds: Optional[float] = None,
        eviction_grace_seconds: Optional[float] = None,
        max_sessions_per_backend: Optional[int] = None,
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
        self._sweep_interval_seconds: float = (
            sweep_interval_seconds
            if sweep_interval_seconds is not None
            else _env_float(
                "PHOENIX_SANDBOX_SWEEP_INTERVAL_SECONDS",
                _DEFAULT_SWEEP_INTERVAL_SECONDS,
            )
        )
        self._eviction_grace_seconds: float = (
            eviction_grace_seconds
            if eviction_grace_seconds is not None
            else _env_float(
                "PHOENIX_SANDBOX_EVICTION_GRACE_SECONDS",
                _DEFAULT_EVICTION_GRACE_SECONDS,
            )
        )
        self._max_sessions_per_backend: int = (
            max_sessions_per_backend
            if max_sessions_per_backend is not None
            else _env_int(
                "PHOENIX_SANDBOX_MAX_SESSIONS_PER_BACKEND",
                _DEFAULT_MAX_SESSIONS_PER_BACKEND,
            )
        )

        # asyncio primitives are loop-bound; construct lazily inside the
        # running loop (start() / acquire()) rather than at __init__ time.
        self._state_lock: Optional[Lock] = None
        self._key_locks: dict[tuple[int, str], Lock] = {}
        self._tracked: dict[tuple[int, str], _TrackedSession] = {}
        # Fire-and-forget eviction tasks scheduled via schedule_eviction.
        # Held adjacent to (not merged into) self._tasks: self._tasks is the
        # daemon body spawned from _run() and is cancelled by DaemonTask.stop;
        # _pending_tasks is awaited (not cancelled) on stop so the underlying
        # backend.stop_session completes rather than being cancelled mid-flight.
        self._pending_tasks: Optional[set[Task[None]]] = None

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

    def schedule_eviction(
        self,
        backend: SandboxBackend,
        session_key: str,
    ) -> None:
        """Schedule a fire-and-forget eviction of (backend, session_key).

        Used by callers that cannot await the eviction inline — notably the
        per-execute timeout teardown in ``CodeEvaluatorRunner.evaluate``,
        which runs in both a GraphQL request scope and the ``ExperimentRunner``
        daemon scope. The created task is retained on the manager so it is
        awaited (not cancelled) during shutdown, completing the underlying
        ``backend.stop_session`` rather than leaking the orphan.
        """
        if isinstance(backend, BaseNoSessionBackend):
            return
        if self._pending_tasks is None:
            self._pending_tasks = set()
        task: Task[None] = asyncio.create_task(self.evict_for_backend_key(backend, session_key))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

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

    async def stop(self) -> None:
        """Drain in-flight sessions, then cancel the sweeper.

        Ordering matters: ``_pending_tasks`` (fire-and-forget evictions
        scheduled via ``schedule_eviction``) are awaited first so the
        underlying ``backend.stop_session`` calls complete instead of being
        cancelled mid-flight; then each unique backend tracked in
        ``_tracked`` is drained via ``evict_for_backend`` (which marks
        in-flight entries for eviction and waits up to
        ``eviction_grace_seconds`` for them to release); finally
        ``super().stop()`` cancels the sweeper task. The combined wall
        clock budget must fit within ``DaemonTask.stop``'s 10s ceiling:
        ``eviction_grace_seconds * N_unique_backends + drain(_pending_tasks)``.
        """
        # Drain pending evictions first — they may already be the call that
        # would have popped a tracked entry. Use gather(return_exceptions=True)
        # under wait_for so a single failing task neither cancels its
        # siblings nor blocks shutdown.
        pending = self._pending_tasks
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=self._eviction_grace_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Pending sandbox eviction tasks did not drain within %.2fs",
                    self._eviction_grace_seconds,
                )
        # Snapshot unique backends under the state lock; iterate evictions
        # outside the lock so each per-backend drain holds only its own
        # per-key locks (avoiding cross-key lock contention during shutdown).
        await self._ensure_state_lock()
        assert self._state_lock is not None
        async with self._state_lock:
            seen_ids: set[int] = set()
            unique_backends: list[SandboxBackend] = []
            for tracked in self._tracked.values():
                bid = id(tracked.backend)
                if bid not in seen_ids:
                    seen_ids.add(bid)
                    unique_backends.append(tracked.backend)
        for backend in unique_backends:
            try:
                await self.evict_for_backend(backend)
            except Exception:
                logger.exception(
                    "Failed to evict sandbox backend during shutdown: backend=%r",
                    type(backend).__name__,
                )
        await super().stop()

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
                # Refuse to admit new callers onto a session that another
                # caller has explicitly invalidated. The entry is draining
                # and will be stopped on its last release; piggy-backing a
                # fresh acquire would extend its lifetime past the stop
                # request. Surface a deterministic error rather than a
                # silent restart so the UI can decide whether to retry.
                if existing.marked_for_eviction:
                    raise SessionInvalidated()
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
        # for eviction. Track the latter (with the tracked instance, not just
        # the key) so the drain poll can identity-check against the same
        # entry — a fresh same-key acquire that pops the marked entry and
        # inserts a new one must not be misread as the marked entry having
        # drained.
        marked: list[tuple[tuple[int, str], _TrackedSession]] = []
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
                    marked.append((key, tracked))
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
                # Identity check: the entry is still draining only if the
                # same ``_TrackedSession`` instance is still under the key.
                # A fresh acquire on the same key inserts a new instance
                # (the old one is popped by its last release) — that fresh
                # entry is not what we marked, so drop it from the wait set.
                marked = [(k, t) for (k, t) in marked if self._tracked.get(k) is t]

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
