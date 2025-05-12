from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseProcessor(ABC):
    @classmethod
    @abstractmethod
    def can_handle(cls, mime: str, ext: str) -> bool:
        """Return True if this processor handles the given file."""
        ...

    @abstractmethod
    def process(self, src_path: str) -> str:
        """Read file at src_path and return extracted/transcribed text."""
        ...

    def postprocess(self, text: str) -> Dict[str, Any]:
        """Optional: chunk, add timestamps, metadata, etc."""
        return {"text": text}