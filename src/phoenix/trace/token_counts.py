import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional

from openinference.semconv.trace import SpanAttributes

from phoenix.trace.attributes import get_attribute_value


@dataclass(frozen=True)
class LLMTokenCounts:
    prompt: Optional[int] = None
    completion: Optional[int] = None
    total: Optional[int] = None
    prompt_details_cache_read: Optional[int] = None
    prompt_details_cache_write: Optional[int] = None
    prompt_details_audio: Optional[int] = None
    completion_details_reasoning: Optional[int] = None
    completion_details_audio: Optional[int] = None


def get_llm_token_counts(attributes: Mapping[str, Any]) -> LLMTokenCounts:
    output_value = _mapping_value(get_attribute_value(attributes, SpanAttributes.OUTPUT_VALUE))
    metadata_usage = _mapping_value(
        get_attribute_value(attributes, (SpanAttributes.METADATA, "usage"))
    )
    output_usage = _mapping_value(_get_nested(output_value, ("usage",)))

    prompt = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_PROMPT,
            ("llm", "token_count", "prompt_tokens"),
            ("llm", "token_count", "input"),
            ("llm", "token_count", "input_tokens"),
        ),
        metadata_usage,
        ("promptTokens", "prompt_tokens", "inputTokens", "input_tokens"),
        output_usage,
        ("prompt_tokens", "input_tokens"),
    )
    completion = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_COMPLETION,
            ("llm", "token_count", "completion_tokens"),
            ("llm", "token_count", "output"),
            ("llm", "token_count", "output_tokens"),
        ),
        metadata_usage,
        ("completionTokens", "completion_tokens", "outputTokens", "output_tokens"),
        output_usage,
        ("completion_tokens", "output_tokens"),
    )
    total = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_TOTAL,
            ("llm", "token_count", "total_tokens"),
        ),
        metadata_usage,
        ("totalTokens", "total_tokens", "total"),
        output_usage,
        ("total_tokens", "total"),
    )
    if total is None:
        total = (prompt or 0) + (completion or 0)
    elif prompt is None and completion is not None:
        prompt = max(0, total - completion)
    elif completion is None and prompt is not None:
        completion = max(0, total - prompt)

    prompt_details_cache_read = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_READ,
            ("llm", "token_count", "cache_read"),
            ("llm", "token_count", "cached_prompt_tokens"),
            ("llm", "token_count", "cached_input_tokens"),
        ),
        metadata_usage,
        ("cachedPromptTokens", "cached_prompt_tokens", "cachedInputTokens", "cached_input_tokens"),
        output_usage,
        (
            ("prompt_tokens_details", "cached_tokens"),
            ("input_tokens_details", "cached_tokens"),
            "cached_prompt_tokens",
            "cached_input_tokens",
        ),
    )
    prompt_details_cache_write = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_PROMPT_DETAILS_CACHE_WRITE,
            ("llm", "token_count", "cache_write"),
        ),
        metadata_usage,
        ("cacheWriteTokens", "cache_write_tokens"),
        output_usage,
        ("cache_write_tokens",),
    )
    prompt_details_audio = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_PROMPT_DETAILS_AUDIO,
            ("llm", "token_count", "prompt_audio"),
            ("llm", "token_count", "input_audio"),
        ),
        metadata_usage,
        ("promptAudioTokens", "inputAudioTokens", "prompt_audio_tokens", "input_audio_tokens"),
        output_usage,
        (
            ("prompt_tokens_details", "audio_tokens"),
            ("input_tokens_details", "audio_tokens"),
        ),
    )
    completion_details_reasoning = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_COMPLETION_DETAILS_REASONING,
            ("llm", "token_count", "reasoning"),
            ("llm", "token_count", "reasoning_tokens"),
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
    completion_details_audio = _first_int(
        attributes,
        (
            SpanAttributes.LLM_TOKEN_COUNT_COMPLETION_DETAILS_AUDIO,
            ("llm", "token_count", "completion_audio"),
            ("llm", "token_count", "output_audio"),
        ),
        metadata_usage,
        (
            "completionAudioTokens",
            "outputAudioTokens",
            "completion_audio_tokens",
            "output_audio_tokens",
        ),
        output_usage,
        (
            ("completion_tokens_details", "audio_tokens"),
            ("output_tokens_details", "audio_tokens"),
        ),
    )

    return LLMTokenCounts(
        prompt=prompt,
        completion=completion,
        total=total,
        prompt_details_cache_read=prompt_details_cache_read,
        prompt_details_cache_write=prompt_details_cache_write,
        prompt_details_audio=prompt_details_audio,
        completion_details_reasoning=completion_details_reasoning,
        completion_details_audio=completion_details_audio,
    )


def _first_int(
    attributes: Mapping[str, Any],
    attribute_keys: Sequence[str | Sequence[str]],
    *mapping_specs: object,
) -> Optional[int]:
    for key in attribute_keys:
        value = _as_non_negative_int(get_attribute_value(attributes, key))
        if value is not None:
            return value
    for mapping, keys in zip(mapping_specs[0::2], mapping_specs[1::2]):
        if not isinstance(mapping, Mapping):
            continue
        if not isinstance(keys, Sequence):
            continue
        for key in keys:
            value = _as_non_negative_int(
                _get_nested(mapping, key if isinstance(key, tuple) else (key,))
            )
            if value is not None:
                return value
    return None


def _mapping_value(value: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except Exception:
            return None
        if isinstance(decoded, Mapping):
            return decoded
    return None


def _get_nested(mapping: Optional[Mapping[str, Any]], keys: Sequence[str]) -> Any:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, Mapping):
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
