from typing import Optional

import strawberry


@strawberry.type
class TokenCountCompletionDetails:
    reasoning: Optional[int]
    audio: Optional[int]
