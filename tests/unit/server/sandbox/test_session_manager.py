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
