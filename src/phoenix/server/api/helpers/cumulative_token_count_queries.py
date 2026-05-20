"""Batched aggregation queries for cumulative token counts.

Both helpers sum cumulative token count columns from root spans
(`Span.parent_id IS NULL`) and group by the requested key. SUM-and-GROUP-BY
is required because a single trace may have multiple root spans, and a single
session may have multiple traces — direct single-row reads would silently
under-report. NULL columns coalesce to 0 so traces/sessions whose root spans
have no LLM descendants return totals of 0 rather than NULL.

Each helper returns a Select with columns: `id_` (the grouping key), `prompt`,
`completion`, `total`, and token detail totals.

Callers must default missing keys to (0, 0). The GROUP BY emits no row for a
key whose set of root spans is empty (e.g., a trace with no spans yet, or a
session whose traces have no `parent_id IS NULL` spans), so a key being absent
from the result set means "zero", not "unknown".
"""

from typing import Any, Collection

from sqlalchemy import Select, func, select
from sqlalchemy.sql.functions import coalesce

from phoenix.db import models


def _cumulative_total_expr() -> Any:
    return coalesce(
        func.nullif(models.Span.cumulative_llm_token_count_total, 0),
        coalesce(models.Span.cumulative_llm_token_count_prompt, 0)
        + coalesce(models.Span.cumulative_llm_token_count_completion, 0),
    )


def cumulative_token_counts_by_session(
    keys: Collection[int],
) -> "Select[Any]":
    """Sum cumulative token counts on root spans, grouped by session rowid.

    Columns: `id_` (project_session_rowid), `prompt`, `completion`.
    """
    return (
        select(
            models.Trace.project_session_rowid.label("id_"),
            func.sum(coalesce(models.Span.cumulative_llm_token_count_prompt, 0)).label("prompt"),
            func.sum(coalesce(models.Span.cumulative_llm_token_count_completion, 0)).label(
                "completion"
            ),
            func.sum(_cumulative_total_expr()).label("total"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_prompt_details_cache_read, 0)
            ).label("cache_read"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_prompt_details_cache_write, 0)
            ).label("cache_write"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_prompt_details_audio, 0)
            ).label("prompt_audio"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_completion_details_reasoning, 0)
            ).label("reasoning"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_completion_details_audio, 0)
            ).label("completion_audio"),
        )
        .join_from(models.Span, models.Trace)
        .where(models.Span.parent_id.is_(None))
        .where(models.Trace.project_session_rowid.in_(keys))
        .group_by(models.Trace.project_session_rowid)
    )


def cumulative_token_counts_by_trace(
    keys: Collection[int],
) -> "Select[Any]":
    """Sum cumulative token counts on root spans, grouped by trace rowid.

    Columns: `id_` (trace_rowid), `prompt`, `completion`.
    """
    return (
        select(
            models.Span.trace_rowid.label("id_"),
            func.sum(coalesce(models.Span.cumulative_llm_token_count_prompt, 0)).label("prompt"),
            func.sum(coalesce(models.Span.cumulative_llm_token_count_completion, 0)).label(
                "completion"
            ),
            func.sum(_cumulative_total_expr()).label("total"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_prompt_details_cache_read, 0)
            ).label("cache_read"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_prompt_details_cache_write, 0)
            ).label("cache_write"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_prompt_details_audio, 0)
            ).label("prompt_audio"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_completion_details_reasoning, 0)
            ).label("reasoning"),
            func.sum(
                coalesce(models.Span.cumulative_llm_token_count_completion_details_audio, 0)
            ).label("completion_audio"),
        )
        .where(models.Span.parent_id.is_(None))
        .where(models.Span.trace_rowid.in_(keys))
        .group_by(models.Span.trace_rowid)
    )
