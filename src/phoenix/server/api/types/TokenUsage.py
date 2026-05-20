import strawberry


@strawberry.type
class TokenUsage:
    prompt: float = 0
    completion: float = 0
    total: float = 0
    cache_read: float = 0
    cache_write: float = 0
    prompt_audio: float = 0
    reasoning: float = 0
    completion_audio: float = 0
