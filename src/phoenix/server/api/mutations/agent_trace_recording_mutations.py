from __future__ import annotations

import strawberry
from starlette.requests import Request
from strawberry.types import Info

from phoenix.server.api.auth import (
    IsAdminIfAuthEnabled,
    IsLocked,
    IsNotReadOnly,
    IsNotViewer,
)
from phoenix.server.api.context import Context
from phoenix.server.api.queries import AgentTraceRecording
from phoenix.server.bearer_auth import PhoenixUser
from phoenix.server.settings.registry import AgentTraceRecordingSetting


@strawberry.input
class SetAgentTraceRecordingInput:
    allow_local_traces: bool
    allow_remote_export: bool


@strawberry.type
class AgentTraceRecordingMutationMixin:
    @strawberry.mutation(
        permission_classes=[IsAdminIfAuthEnabled, IsNotReadOnly, IsNotViewer, IsLocked]
    )  # type: ignore
    async def set_agent_trace_recording(
        self,
        info: Info[Context, None],
        input: SetAgentTraceRecordingInput,
    ) -> AgentTraceRecording:
        assert isinstance(request := info.context.request, Request)
        user_id: int | None = None
        if "user" in request.scope and isinstance((user := info.context.user), PhoenixUser):
            user_id = int(user.identity)
        await info.context.settings.update_agent_trace_recording(
            AgentTraceRecordingSetting(
                allow_local_traces=input.allow_local_traces,
                allow_remote_export=input.allow_remote_export,
            ),
            user_id=user_id,
        )
        recording = info.context.settings.agent_trace_recording
        return AgentTraceRecording(
            allow_local_traces=recording.allow_local_traces,
            allow_remote_export=recording.allow_remote_export,
        )
