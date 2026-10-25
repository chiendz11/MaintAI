
from typing import Optional


class QueryBuilder:
    def build(
        self,
        querytext: str,
        error_code: Optional[str] = None
    ) -> str:
        querytext = (querytext or "").strip()
        error_code = (error_code or "").strip()

        if not querytext and not error_code:
            raise ValueError(
                "Phải có querytext hoặc error_code"
            )

        parts = []

        if error_code:
            parts.append(error_code)

        if querytext:
            parts.append(querytext)

        return ". ".join(parts)
