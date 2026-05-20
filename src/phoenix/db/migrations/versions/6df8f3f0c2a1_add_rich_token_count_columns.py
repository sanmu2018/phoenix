"""add rich token count columns

Revision ID: 6df8f3f0c2a1
Revises: 575aa27302ee
Create Date: 2026-05-20 00:00:00.000000

"""

import json
from collections import defaultdict
from typing import Any, Optional, Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "6df8f3f0c2a1"
down_revision: Union[str, None] = "575aa27302ee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "spans",
        sa.Column(
            "cumulative_llm_token_count_total",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "spans",
        sa.Column(
            "cumulative_llm_token_count_prompt_details_cache_read",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "spans",
        sa.Column(
            "cumulative_llm_token_count_prompt_details_cache_write",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "spans",
        sa.Column(
            "cumulative_llm_token_count_prompt_details_audio",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "spans",
        sa.Column(
            "cumulative_llm_token_count_completion_details_reasoning",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "spans",
        sa.Column(
            "cumulative_llm_token_count_completion_details_audio",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column("spans", sa.Column("llm_token_count_total", sa.Integer, nullable=True))

    try:
        op.drop_index("ix_cumulative_llm_token_count_total", table_name="spans")
    except Exception:
        pass
    _backfill_token_counts()
    op.create_index(
        "ix_cumulative_llm_token_count_total",
        "spans",
        [sa.text("cumulative_llm_token_count_total")],
        unique=False,
    )


def downgrade() -> None:
    try:
        op.drop_index("ix_cumulative_llm_token_count_total", table_name="spans")
    except Exception:
        pass
    op.create_index(
        "ix_cumulative_llm_token_count_total",
        "spans",
        [sa.text("(cumulative_llm_token_count_prompt + cumulative_llm_token_count_completion)")],
        unique=False,
    )
    op.drop_column("spans", "llm_token_count_total")
    op.drop_column("spans", "cumulative_llm_token_count_completion_details_audio")
    op.drop_column("spans", "cumulative_llm_token_count_completion_details_reasoning")
    op.drop_column("spans", "cumulative_llm_token_count_prompt_details_audio")
    op.drop_column("spans", "cumulative_llm_token_count_prompt_details_cache_write")
    op.drop_column("spans", "cumulative_llm_token_count_prompt_details_cache_read")
    op.drop_column("spans", "cumulative_llm_token_count_total")


def _backfill_token_counts() -> None:
    bind = op.get_bind()
    spans = sa.table(
        "spans",
        sa.column("id", sa.Integer),
        sa.column("trace_rowid", sa.Integer),
        sa.column("span_id", sa.String),
        sa.column("parent_id", sa.String),
        sa.column("attributes", sa.JSON),
        sa.column("llm_token_count_prompt", sa.Integer),
        sa.column("llm_token_count_completion", sa.Integer),
        sa.column("llm_token_count_total", sa.Integer),
        sa.column("cumulative_llm_token_count_total", sa.Integer),
        sa.column("cumulative_llm_token_count_prompt_details_cache_read", sa.Integer),
        sa.column("cumulative_llm_token_count_prompt_details_cache_write", sa.Integer),
        sa.column("cumulative_llm_token_count_prompt_details_audio", sa.Integer),
        sa.column("cumulative_llm_token_count_completion_details_reasoning", sa.Integer),
        sa.column("cumulative_llm_token_count_completion_details_audio", sa.Integer),
    )
    rows = [dict(row) for row in bind.execute(sa.select(spans)).mappings()]
    if not rows:
        return

    by_id = {row["id"]: row for row in rows}
    children: dict[tuple[int, Optional[str]], list[int]] = defaultdict(list)
    self_counts: dict[int, dict[str, int]] = {}
    cumulative_counts: dict[int, dict[str, int]] = {}
    for row in rows:
        children[(row["trace_rowid"], row["parent_id"])].append(row["id"])
        self_counts[row["id"]] = _extract_counts(row)

    visiting: set[int] = set()

    def visit(row_id: int) -> dict[str, int]:
        if row_id in cumulative_counts:
            return cumulative_counts[row_id]
        if row_id in visiting:
            return self_counts[row_id]
        visiting.add(row_id)
        row = by_id[row_id]
        total = dict(self_counts[row_id])
        for child_id in children.get((row["trace_rowid"], row["span_id"]), []):
            for key, value in visit(child_id).items():
                total[key] += value
        cumulative_counts[row_id] = total
        visiting.remove(row_id)
        return total

    for row_id in by_id:
        visit(row_id)

    for row_id, cumulative in cumulative_counts.items():
        self_count = self_counts[row_id]
        bind.execute(
            spans.update()
            .where(spans.c.id == row_id)
            .values(
                llm_token_count_total=self_count["total"] or None,
                cumulative_llm_token_count_total=cumulative["total"],
                cumulative_llm_token_count_prompt_details_cache_read=cumulative["cache_read"],
                cumulative_llm_token_count_prompt_details_cache_write=cumulative["cache_write"],
                cumulative_llm_token_count_prompt_details_audio=cumulative["prompt_audio"],
                cumulative_llm_token_count_completion_details_reasoning=cumulative["reasoning"],
                cumulative_llm_token_count_completion_details_audio=cumulative["completion_audio"],
            )
        )


def _extract_counts(row: dict[str, Any]) -> dict[str, int]:
    attributes = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
    metadata_usage = _mapping_value(_get(attributes, ("metadata", "usage")))
    output_usage = _mapping_value(
        _get(_mapping_value(_get(attributes, ("output", "value"))), ("usage",))
    )
    prompt = _first_int(
        attributes,
        ("llm.token_count.prompt", "llm.token_count.prompt_tokens", "llm.token_count.input_tokens"),
        metadata_usage,
        ("promptTokens", "prompt_tokens", "inputTokens", "input_tokens"),
        output_usage,
        ("prompt_tokens", "input_tokens"),
    )
    if prompt is None:
        prompt = _as_non_negative_int(row.get("llm_token_count_prompt"))
    completion = _first_int(
        attributes,
        (
            "llm.token_count.completion",
            "llm.token_count.completion_tokens",
            "llm.token_count.output_tokens",
        ),
        metadata_usage,
        ("completionTokens", "completion_tokens", "outputTokens", "output_tokens"),
        output_usage,
        ("completion_tokens", "output_tokens"),
    )
    if completion is None:
        completion = _as_non_negative_int(row.get("llm_token_count_completion"))
    total = _first_int(
        attributes,
        ("llm.token_count.total", "llm.token_count.total_tokens"),
        metadata_usage,
        ("totalTokens", "total_tokens", "total"),
        output_usage,
        ("total_tokens", "total"),
    )
    if total is None:
        total = (prompt or 0) + (completion or 0)

    return {
        "total": total or 0,
        "cache_read": _first_int(
            attributes,
            (
                "llm.token_count.prompt_details.cache_read",
                "llm.token_count.cache_read",
                "llm.token_count.cached_prompt_tokens",
                "llm.token_count.cached_input_tokens",
            ),
            metadata_usage,
            (
                "cachedPromptTokens",
                "cached_prompt_tokens",
                "cachedInputTokens",
                "cached_input_tokens",
            ),
            output_usage,
            (("prompt_tokens_details", "cached_tokens"), ("input_tokens_details", "cached_tokens")),
        )
        or 0,
        "cache_write": _first_int(
            attributes,
            ("llm.token_count.prompt_details.cache_write", "llm.token_count.cache_write"),
            metadata_usage,
            ("cacheWriteTokens", "cache_write_tokens"),
            output_usage,
            ("cache_write_tokens",),
        )
        or 0,
        "prompt_audio": _first_int(
            attributes,
            ("llm.token_count.prompt_details.audio", "llm.token_count.prompt_audio"),
            output_usage,
            (("prompt_tokens_details", "audio_tokens"), ("input_tokens_details", "audio_tokens")),
        )
        or 0,
        "reasoning": _first_int(
            attributes,
            (
                "llm.token_count.completion_details.reasoning",
                "llm.token_count.reasoning",
                "llm.token_count.reasoning_tokens",
            ),
            metadata_usage,
            (
                "reasoningTokens",
                "reasoning_tokens",
                "reasoningOutputTokens",
                "reasoning_output_tokens",
            ),
            output_usage,
            (
                ("completion_tokens_details", "reasoning_tokens"),
                ("output_tokens_details", "reasoning_tokens"),
                "reasoning_tokens",
            ),
        )
        or 0,
        "completion_audio": _first_int(
            attributes,
            ("llm.token_count.completion_details.audio", "llm.token_count.completion_audio"),
            output_usage,
            (
                ("completion_tokens_details", "audio_tokens"),
                ("output_tokens_details", "audio_tokens"),
            ),
        )
        or 0,
    }


def _first_int(
    attributes: dict[str, Any],
    attribute_keys: Sequence[str | Sequence[str]],
    *mapping_specs: Any,
) -> Optional[int]:
    for key in attribute_keys:
        value = _as_non_negative_int(
            _get(attributes, key.split(".") if isinstance(key, str) else key)
        )
        if value is not None:
            return value
    for mapping, keys in zip(mapping_specs[0::2], mapping_specs[1::2]):
        if not isinstance(mapping, dict) or not isinstance(keys, tuple):
            continue
        for key in keys:
            value = _as_non_negative_int(_get(mapping, key if isinstance(key, tuple) else (key,)))
            if value is not None:
                return value
    return None


def _mapping_value(value: Any) -> Optional[dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except Exception:
            return None
        return decoded if isinstance(decoded, dict) else None
    return None


def _get(mapping: Optional[dict[str, Any]], keys: Sequence[str]) -> Any:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _as_non_negative_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return None
