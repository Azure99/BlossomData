from abc import ABC, abstractmethod
from typing import Any

from blossom.schema.schema import Schema


class DataHandler(ABC):
    def from_dict_list(self, data: list[dict[str, Any]]) -> list[Schema]:
        return [self.from_dict(row) for row in data]

    def to_dict_list(self, data: list[Schema]) -> list[dict[str, Any]]:
        return [self.to_dict(row) for row in data]

    @abstractmethod
    def from_dict(self, data: dict[str, Any]) -> Schema:
        pass

    @abstractmethod
    def to_dict(self, schema: Schema) -> dict[str, Any]:
        pass
