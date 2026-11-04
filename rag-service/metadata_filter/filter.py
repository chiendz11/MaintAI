
from typing import Any, Dict, Optional


class MetadataFilter:
    ALLOWED_FIELDS = {
        "machine_id",
        "machine_model",
        "manufacturer",
        "document_type",
        "source",
    }
    def build(
        self,
        machine_id: Optional[str] = None,
        machine_model: Optional[str] = None,
        manufacturer: Optional[str] = None,
        document_type: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Dict[str, Any]:
        candidates = {
            "machine_id": machine_id,
            "machine_model": machine_model,
            "manufacturer": manufacturer,
            "document_type": document_type,
            "source": source,
        }
        filters = {}
        for key, value in candidates.items():
            if key not in self.ALLOWED_FIELDS:
                continue
            if value is None:
                continue
            if not isinstance(value, str):
                raise ValueError(
                    f"{key} phải là chuỗi"
                )
            value = value.strip()
            if value:
                filters[key] = value
        return filters
