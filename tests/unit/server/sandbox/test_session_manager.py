"""Unit tests for SandboxSessionManager — happy-path coverage of each
verification bullet in Phase 1 of the
``sandbox-session-manager-playground-session-reuse`` work item.

Test surface (one happy-path test per verification bullet):
- ``test_acquire_populates_session_on_first_acquire_and_reuses``
- ``test_idle_ttl_evicts_when_inflight_zero``
- ``test_idle_ttl_does_not_evict_when_inflight_nonzero``
- ``test_acquire_raises_session_limit_exceeded``
- ``test_no_op_for_stateless_backend``
- ``test_daemontask_stop_cancels_sweeper_within_grace``
"""

from __future__ import annotations

import asyncio
from asyncio import Event
from typing import AsyncIterator, Optional
from unittest.mock import patch

import pytest

from phoenix.server.sandbox.session_manager import (
    SandboxSessionManager,
    SessionInvalidated,
    SessionLimitExceeded,
)
from phoenix.server.sandbox.types import (
    BaseNoSessionBackend,
    ExecutionResult,
    SandboxBackend,
)


class _FakeBackend(SandboxBackend):
    """In-memory session-capable fake backend with start/stop call counters."""

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
        return ExecutionResult(stdout=code, stderr="")

    async def close(self) -> None:
        self._sessions.clear()


class _StatelessFakeBackend(BaseNoSessionBackend):
    """Stateless fake backend — start/stop are inherited no-ops."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, str, Optional[int]]] = []

    async def execute(
        self,
        code: str,
        session_key: str,
        timeout: Optional[int] = None,
    ) -> ExecutionResult:
        self.execute_calls.append((code, session_key, timeout))
        return ExecutionResult(stdout=code, stderr="")

    async def close(self) -> None:
        pass


@pytest.fixture
async def sweep_trigger() -> AsyncIterator[Event]:
    """Patch session_manager.sleep with an Event-gated wait_for_event so
    tests can drive the sweeper loop one tick at a time.

    Mirrors the canonical pattern in
    ``tests/unit/server/daemons/test_generative_model_store.py``.
    """
    event = Event()

    async def wait_for_event(seconds: float) -> None:
        await event.wait()
        event.clear()

    with patch("phoenix.server.sandbox.session_manager.sleep", wait_for_event):
        yield event


@pytest.mark.asyncio
async def test_acquire_populates_session_on_first_acquire_and_reuses() -> None:
    """First acquire calls backend.start_session; second acquire of the same
    key reuses — start_session called exactly once across two acquires."""
    manager = SandboxSessionManager()
    backend = _FakeBackend()

    async with manager.acquire(backend, "k1") as session:
        result = await session.execute("print(1)")
    assert result.stdout == "print(1)"

    async with manager.acquire(backend, "k1") as session:
        await session.execute("print(2)")

    assert backend.start_calls == ["k1"], "start_session must be called exactly once"
    assert len(backend.execute_calls) == 2


@pytest.mark.asyncio
async def test_idle_ttl_evicts_when_inflight_zero(sweep_trigger: Event) -> None:
    """Idle TTL with in_flight_count==0 → sweep evicts and calls
    backend.stop_session."""
    manager = SandboxSessionManager(
        idle_ttl_seconds=0.0,
        sweep_interval_seconds=0.0,
    )
    backend = _FakeBackend()

    async with manager.acquire(backend, "k1"):
        pass

    await manager.start()
    try:
        await asyncio.sleep(0)
        # Wait briefly so monotonic() advances past last_used + ttl (0.0).
        await asyncio.sleep(0.01)
        # Trigger one sweep tick — sleep() is patched to wait on this Event.
        sweep_trigger.set()
        # Yield to let the sweeper loop process. Poll for stop_session call.
        for _ in range(50):
            await asyncio.sleep(0.01)
            if backend.stop_calls:
                break
    finally:
        await manager.stop()

    assert backend.stop_calls == ["k1"], (
        f"sweeper must stop idle session; got stop_calls={backend.stop_calls}"
    )


@pytest.mark.asyncio
async def test_idle_ttl_does_not_evict_when_inflight_nonzero(
    sweep_trigger: Event,
) -> None:
    """Idle TTL with in_flight_count>0 → sweep skips; stop_session not called."""
    manager = SandboxSessionManager(
        idle_ttl_seconds=0.0,
        sweep_interval_seconds=0.0,
    )
    backend = _FakeBackend()

    await manager.start()
    try:
        async with manager.acquire(backend, "k1"):
            # Inside the acquire context the in-flight count is nonzero.
            sweep_trigger.set()
            # Give the sweeper time to actually attempt a sweep.
            for _ in range(20):
                await asyncio.sleep(0.01)
        # Now we are outside the acquire context — but assertion is about
        # the state during the sweep.
        assert backend.stop_calls == [], (
            f"sweeper must NOT stop in-flight session; got stop_calls={backend.stop_calls}"
        )
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_acquire_raises_session_limit_exceeded() -> None:
    """At max_sessions_per_backend, a new acquire raises SessionLimitExceeded
    and the manager remains usable for existing keys."""
    manager = SandboxSessionManager(max_sessions_per_backend=2)
    backend = _FakeBackend()

    async with manager.acquire(backend, "k1"):
        async with manager.acquire(backend, "k2"):
            with pytest.raises(SessionLimitExceeded):
                async with manager.acquire(backend, "k3"):
                    pytest.fail("acquire should have raised before entering body")

            # Existing keys are still usable.
            async with manager.acquire(backend, "k1") as session:
                await session.execute("print('still works')")

    # start_session called once each for k1 and k2; k3 never reached start.
    assert sorted(backend.start_calls) == ["k1", "k2"]


@pytest.mark.asyncio
async def test_concurrent_acquires_respect_capacity_under_race() -> None:
    """Regression: concurrent acquires for *different* keys on the same
    backend can't both pass the capacity check.

    Per-key locks alone don't serialize capacity accounting — two different
    keys hold different per-key locks and can both yield on
    ``backend.start_session`` before either inserts into ``_tracked``. The
    check + reservation must run under ``_state_lock`` so the second acquire
    sees the first's reservation immediately.
    """
    a_in_start = asyncio.Event()
    release_a = asyncio.Event()

    class _GatedBackend(SandboxBackend):
        def __init__(self) -> None:
            self.start_calls: list[str] = []

        async def start_session(self, session_key: str) -> None:
            self.start_calls.append(session_key)
            if session_key == "a":
                a_in_start.set()
                await release_a.wait()

        async def stop_session(self, session_key: str) -> None:
            pass

        async def execute(
            self,
            code: str,
            session_key: str,
            timeout: Optional[int] = None,
        ) -> ExecutionResult:
            return ExecutionResult(stdout="", stderr="")

        async def close(self) -> None:
            pass

    backend = _GatedBackend()
    manager = SandboxSessionManager(max_sessions_per_backend=1)

    async def acquire_key(key: str) -> str:
        try:
            async with manager.acquire(backend, key):
                return "acquired"
        except SessionLimitExceeded:
            return "rejected"

    # Task A enters acquire and parks inside backend.start_session("a")
    # AFTER reserving the slot under _state_lock.
    task_a = asyncio.create_task(acquire_key("a"))
    await a_in_start.wait()

    # Task B attempts to acquire a different key on the same backend. With
    # the fix it must see A's reservation under _state_lock and be rejected
    # before ever calling backend.start_session("b"). Without the fix, B
    # would pass the capacity check (because A hasn't inserted yet under the
    # old code's logic) and proceed to start_session, exceeding the limit.
    task_b = asyncio.create_task(acquire_key("b"))
    result_b = await task_b

    release_a.set()
    result_a = await task_a

    assert result_a == "acquired"
    assert result_b == "rejected"
    # The fix is observable here: start_session must never be called for "b".
    assert backend.start_calls == ["a"]


@pytest.mark.asyncio
async def test_start_session_failure_releases_reservation() -> None:
    """If ``backend.start_session`` raises, the reserved slot must be popped
    so the next acquire on the same key starts cleanly and capacity
    accounting stays correct.
    """

    class _FailingBackend(SandboxBackend):
        def __init__(self) -> None:
            self.start_calls: list[str] = []
            self.fail_next = True

        async def start_session(self, session_key: str) -> None:
            self.start_calls.append(session_key)
            if self.fail_next:
                self.fail_next = False
                raise RuntimeError("simulated start failure")

        async def stop_session(self, session_key: str) -> None:
            pass

        async def execute(
            self,
            code: str,
            session_key: str,
            timeout: Optional[int] = None,
        ) -> ExecutionResult:
            return ExecutionResult(stdout=code, stderr="")

        async def close(self) -> None:
            pass

    backend = _FailingBackend()
    manager = SandboxSessionManager(max_sessions_per_backend=1)

    with pytest.raises(RuntimeError, match="simulated start failure"):
        async with manager.acquire(backend, "k1"):
            pytest.fail("acquire body should not run when start_session fails")

    # Reservation must have been rolled back — _tracked is empty, so the
    # capacity slot is free and a fresh acquire starts a new session.
    assert manager._tracked == {}
    # D3's rollback-path lock pop: _key_locks entry must also be cleared
    # so a failure storm doesn't slow-leak lock entries.
    assert manager._key_locks == {}

    async with manager.acquire(backend, "k1") as session:
        result = await session.execute("print('ok')")
    assert result.stdout == "print('ok')"
    assert backend.start_calls == ["k1", "k1"]


@pytest.mark.asyncio
async def test_key_locks_no_unbounded_growth_across_acquire_evict_cycles() -> None:
    """D3 regression: ``_key_locks`` entries must be popped alongside
    ``_tracked`` entries on every eviction site. Without the pop, the dict
    grows unboundedly across the process lifetime because the frontend mints
    a fresh UUID per component mount.

    Drives 100 acquire/evict/acquire cycles on distinct keys and asserts
    ``len(_key_locks) <= 1`` — zero or one residual entry from the last
    iteration, never N.
    """
    manager = SandboxSessionManager(max_sessions_per_backend=4)
    backend = _FakeBackend()

    for i in range(100):
        key = f"k{i}"
        async with manager.acquire(backend, key):
            pass
        await manager.evict_for_backend_key(backend, key)

    assert len(manager._key_locks) <= 1, (
        f"_key_locks should not grow unboundedly; got {len(manager._key_locks)} entries"
    )
    assert len(manager._tracked) <= 1, (
        f"_tracked should not grow unboundedly; got {len(manager._tracked)} entries"
    )


@pytest.mark.asyncio
async def test_release_after_lock_pop_still_decrements_inflight() -> None:
    """D1 regression: a coroutine that captured the per-key lock reference
    before a concurrent eviction popped ``_key_locks[key]`` must still
    decrement ``in_flight_count`` correctly on release.

    Drives the race: coroutine X acquires k1, exits cleanly. Then evict
    pops both ``_tracked`` and ``_key_locks`` for k1. Coroutine Y was
    waiting on the same per-key lock object across the pop. With the old
    ``_release(key)`` signature, Y's release would do
    ``self._key_locks.get(key)`` → None → return early, leaking the
    in-flight count. With the D1 fix (``_release(key, key_lock)`` threaded
    through), Y's release uses its captured lock reference and decrements
    correctly.
    """
    manager = SandboxSessionManager(max_sessions_per_backend=2)
    backend = _FakeBackend()

    # X acquires and exits cleanly. _tracked stays (no marked_for_eviction).
    async with manager.acquire(backend, "k1"):
        pass

    # Capture X's per-key lock reference BEFORE eviction pops it.
    key = (id(backend), "k1")
    captured_lock = manager._key_locks[key]

    # Evict pops both _tracked[k1] and _key_locks[k1] (in_flight==0).
    await manager.evict_for_backend_key(backend, "k1")
    assert key not in manager._key_locks
    assert key not in manager._tracked
    assert backend.stop_calls == ["k1"]

    # Now Y enters acquire fresh — gets a NEW _key_locks entry. After Y's
    # release, _tracked and _key_locks should be consistent (in_flight
    # decremented, residual entries optional).
    async with manager.acquire(backend, "k1"):
        # Inside Y's body the per-key lock entry is the fresh one, not
        # captured_lock.
        assert manager._key_locks[key] is not captured_lock
        tracked = manager._tracked[key]
        assert tracked.in_flight_count == 1
    # After Y's release, in_flight_count must be back to 0 — the release
    # was through the new lock and the new tracked entry. _tracked still
    # holds the entry (no eviction marker) with in_flight_count == 0.
    assert manager._tracked[key].in_flight_count == 0


@pytest.mark.asyncio
async def test_same_key_parallel_acquires_under_popped_lock_waits_for_single_start() -> None:
    """D2 regression: two acquires for the same key racing across a popped
    ``_key_locks`` entry must result in exactly one ``start_session`` call
    AND the follower's body must not enter until the leader's start_session
    has returned.

    Sequence driven:
    1. Leader X acquires k1, exits — release pops both dicts (marked for
       eviction via evict_for_backend_key).
    2. Y begins a new acquire for k1; gates inside ``backend.start_session``
       on an Event so we can observe ordering.
    3. Z begins a parallel acquire for k1 while Y is still parked in
       start_session. Y is leader (is_new=True); Z is follower
       (is_new=False) because Y inserted under _state_lock.
    4. Assert Z does not enter its body before Y's start_session completes.
    5. Release the gate; both complete.
    6. Assert ``backend.start_calls.count("k1") == 1`` for the second-round
       race (Y's start; Z reuses).
    """

    start_gate = asyncio.Event()
    leader_in_start = asyncio.Event()
    follower_entered_body = asyncio.Event()

    class _GatedBackend(SandboxBackend):
        def __init__(self) -> None:
            self.start_calls: list[str] = []
            self.stop_calls: list[str] = []

        async def start_session(self, session_key: str) -> None:
            self.start_calls.append(session_key)
            if session_key == "k1" and len(self.start_calls) >= 2:
                # Second start_session call (Y's) — park here.
                leader_in_start.set()
                await start_gate.wait()

        async def stop_session(self, session_key: str) -> None:
            self.stop_calls.append(session_key)

        async def execute(
            self,
            code: str,
            session_key: str,
            timeout: Optional[int] = None,
        ) -> ExecutionResult:
            return ExecutionResult(stdout=code, stderr="")

        async def close(self) -> None:
            pass

    backend = _GatedBackend()
    manager = SandboxSessionManager(max_sessions_per_backend=4)

    # Step 1: X acquires and is then evicted; both dicts pop.
    async with manager.acquire(backend, "k1"):
        pass
    await manager.evict_for_backend_key(backend, "k1")
    key = (id(backend), "k1")
    assert key not in manager._key_locks
    assert key not in manager._tracked

    # Step 2: Y begins acquire — leader. Will park in start_session.
    async def y_acquire() -> str:
        async with manager.acquire(backend, "k1"):
            return "y-acquired"

    task_y = asyncio.create_task(y_acquire())
    await leader_in_start.wait()

    # Step 3: Z begins acquire — follower. Should block on start_ready.
    async def z_acquire() -> str:
        async with manager.acquire(backend, "k1"):
            follower_entered_body.set()
            return "z-acquired"

    task_z = asyncio.create_task(z_acquire())

    # Yield repeatedly to give Z a chance to advance — but assert it does
    # NOT enter its body while Y is parked.
    for _ in range(20):
        await asyncio.sleep(0.01)
    assert not follower_entered_body.is_set(), (
        "follower must NOT enter body before leader's start_session resolves"
    )

    # Step 5: release gate; both should complete.
    start_gate.set()
    result_y = await task_y
    result_z = await task_z
    assert result_y == "y-acquired"
    assert result_z == "z-acquired"

    # Step 6: Y's start_session was called exactly once for k1 in the
    # second race round. Total starts: X's k1 + Y's k1 == 2.
    assert backend.start_calls.count("k1") == 2, (
        f"start_session must be called once for X and once for Y, never for Z; "
        f"got start_calls={backend.start_calls}"
    )


@pytest.mark.asyncio
async def test_no_op_for_stateless_backend() -> None:
    """BaseNoSessionBackend → acquire bypasses lock/state tracking entirely;
    backend.execute is invoked, no entries are added to manager state."""
    manager = SandboxSessionManager()
    backend = _StatelessFakeBackend()

    async with manager.acquire(backend, "k1") as session:
        result = await session.execute("print('hi')")

    assert result.stdout == "print('hi')"
    assert backend.execute_calls == [("print('hi')", "k1", None)]
    # Manager did not track any state for the stateless backend.
    assert manager._tracked == {}
    assert manager._key_locks == {}


@pytest.mark.asyncio
async def test_daemontask_stop_cancels_sweeper_within_grace(
    sweep_trigger: Event,
) -> None:
    """DaemonTask.stop() cancels the sweeper task; no dangling sessions."""
    manager = SandboxSessionManager(
        idle_ttl_seconds=60.0,
        sweep_interval_seconds=0.0,
    )
    backend = _FakeBackend()

    await manager.start()
    # Run at least one sweep tick to confirm the sweeper is alive.
    sweep_trigger.set()
    await asyncio.sleep(0.01)

    async with manager.acquire(backend, "k1"):
        pass  # session is tracked but not expired

    await manager.stop()

    # Sweeper task is fully done and cleared.
    assert manager._tasks == []
    # Stop must complete within the DaemonTask 10s grace; if we got here we passed.


@pytest.mark.asyncio
async def test_acquire_raises_session_invalidated_when_marked_for_eviction() -> None:
    """An existing tracked entry marked for eviction must refuse new admits.

    Drives the race: a long-running acquire holds an in-flight session;
    a concurrent ``evict_for_backend_key`` marks the entry for eviction
    (cannot stop synchronously because in_flight > 0); a fresh acquire
    on the same key must observe ``marked_for_eviction`` and raise
    ``SessionInvalidated`` rather than piggy-backing onto the drained-out
    entry.
    """
    leader_in_body = asyncio.Event()
    release_leader = asyncio.Event()

    manager = SandboxSessionManager(
        max_sessions_per_backend=4,
        eviction_grace_seconds=0.0,
    )
    backend = _FakeBackend()

    async def long_acquire() -> None:
        async with manager.acquire(backend, "k1"):
            leader_in_body.set()
            await release_leader.wait()

    leader = asyncio.create_task(long_acquire())
    await leader_in_body.wait()

    # Mark the entry for eviction — in-flight > 0, so the entry stays
    # tracked with marked_for_eviction=True and the eviction-grace poll
    # returns immediately (grace=0.0).
    await manager.evict_for_backend_key(backend, "k1")

    # Fresh acquire on the same key must refuse rather than admit.
    with pytest.raises(SessionInvalidated):
        async with manager.acquire(backend, "k1"):
            pytest.fail("acquire should refuse admit onto a marked entry")

    release_leader.set()
    await leader

    # start_session was called once (for the leader) and never re-driven
    # for the rejected admit.
    assert backend.start_calls == ["k1"]


@pytest.mark.asyncio
async def test_manager_stop_drains_tracked_via_backend_stop_session() -> None:
    """``SandboxSessionManager.stop`` must evict tracked entries before
    cancelling the sweeper — backend.stop_session is called for every
    tracked (backend, key) pair, then the sweeper task is gone.
    """
    manager = SandboxSessionManager(
        idle_ttl_seconds=60.0,
        sweep_interval_seconds=60.0,
        eviction_grace_seconds=0.5,
    )
    backend_a = _FakeBackend()
    backend_b = _FakeBackend()

    await manager.start()
    try:
        async with manager.acquire(backend_a, "ka1"):
            pass
        async with manager.acquire(backend_a, "ka2"):
            pass
        async with manager.acquire(backend_b, "kb1"):
            pass
    finally:
        await manager.stop()

    assert sorted(backend_a.stop_calls) == ["ka1", "ka2"], (
        f"backend_a.stop_session must be called for both keys; got {backend_a.stop_calls}"
    )
    assert backend_b.stop_calls == ["kb1"], (
        f"backend_b.stop_session must be called for its key; got {backend_b.stop_calls}"
    )
    assert manager._tasks == []


@pytest.mark.asyncio
async def test_manager_stop_awaits_pending_tasks_before_cancelling_sweeper() -> None:
    """``schedule_eviction`` tasks must be awaited (not cancelled) by
    ``stop`` so the underlying ``backend.stop_session`` completes.
    """
    stop_session_entered = asyncio.Event()
    release_stop = asyncio.Event()
    stop_session_completed = asyncio.Event()

    class _SlowStopBackend(SandboxBackend):
        def __init__(self) -> None:
            self.start_calls: list[str] = []
            self.stop_calls: list[str] = []

        async def start_session(self, session_key: str) -> None:
            self.start_calls.append(session_key)

        async def stop_session(self, session_key: str) -> None:
            stop_session_entered.set()
            await release_stop.wait()
            self.stop_calls.append(session_key)
            stop_session_completed.set()

        async def execute(
            self,
            code: str,
            session_key: str,
            timeout: Optional[int] = None,
        ) -> ExecutionResult:
            return ExecutionResult(stdout="", stderr="")

        async def close(self) -> None:
            pass

    backend = _SlowStopBackend()
    manager = SandboxSessionManager(
        idle_ttl_seconds=60.0,
        sweep_interval_seconds=60.0,
        eviction_grace_seconds=1.0,
    )

    await manager.start()
    async with manager.acquire(backend, "k1"):
        pass

    # Fire-and-forget eviction parked inside backend.stop_session.
    manager.schedule_eviction(backend, "k1")
    await stop_session_entered.wait()

    async def run_stop() -> None:
        await manager.stop()

    stop_task = asyncio.create_task(run_stop())
    # Give stop() a chance to begin its drain; assert it has NOT cancelled
    # the in-flight stop_session.
    await asyncio.sleep(0.05)
    assert not stop_session_completed.is_set()

    # Release the in-flight stop_session; stop() must observe completion.
    release_stop.set()
    await stop_task

    assert backend.stop_calls == ["k1"], (
        f"backend.stop_session must complete before stop returns; got {backend.stop_calls}"
    )


@pytest.mark.asyncio
async def test_evict_drain_poll_identity_check_rejects_fresh_same_key_entry() -> None:
    """The eviction drain-poll must identity-check the tracked entry —
    a fresh same-key acquire after the marked entry drains and pops
    must not be misread as the marked entry still in flight (or worse,
    as having drained when it was actually a new admit).

    Drives the race:
    1. Acquire k1 — entry E1 inserted, in_flight=1.
    2. evict_for_backend_key(k1) starts — E1 in_flight > 0 so it's
       marked_for_eviction; evict enters its drain-poll loop.
    3. Release E1 — _release pops E1 from _tracked and calls stop_session.
    4. Schedule a fresh acquire on k1 — inserts a NEW entry E2.
    5. Evict's drain-poll runs identity check: ``_tracked.get(key) is E1``
       returns False (E2 is there), so E1 is correctly removed from
       the wait set and the drain returns.

    Observable: the evict call returns within the grace window AND
    the fresh acquire does not raise (E2 is not marked_for_eviction).
    """
    in_first_body = asyncio.Event()
    release_first = asyncio.Event()

    manager = SandboxSessionManager(
        max_sessions_per_backend=4,
        eviction_grace_seconds=1.0,
    )
    backend = _FakeBackend()

    async def first_acquire() -> None:
        async with manager.acquire(backend, "k1"):
            in_first_body.set()
            await release_first.wait()

    first = asyncio.create_task(first_acquire())
    await in_first_body.wait()

    async def evict() -> None:
        await manager.evict_for_backend_key(backend, "k1")

    evict_task = asyncio.create_task(evict())
    # Yield so evict enters its drain-poll loop.
    await asyncio.sleep(0.02)

    # Release first: _release pops E1 from _tracked and calls stop_session.
    release_first.set()
    await first

    # Fresh acquire inserts a NEW entry on the same key. The identity check
    # in the drain-poll must see this is NOT E1 (which was popped) and let
    # the evict_task return.
    async with manager.acquire(backend, "k1") as session:
        result = await session.execute("print('post-evict')")

    # Evict must have completed (drain-poll did not get stuck on the
    # fresh entry mistakenly treated as the marked one).
    await asyncio.wait_for(evict_task, timeout=2.0)

    assert result.stdout == "print('post-evict')"
    # backend.stop_session was called exactly once — for E1 on release.
    assert backend.stop_calls == ["k1"]
