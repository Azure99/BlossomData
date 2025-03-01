from typing import Optional

from blossom.conf.config import Config
from blossom.pipeline.pipeline import Pipeline
from blossom.schema.schema import Schema


class SimplePipeline(Pipeline):
    def __init__(self, config: Optional[Config] = None):
        super().__init__(config)

    def execute(self, data: list[Schema]) -> list[Schema]:
        for operator in self.operators:
            data = operator.process(data)
        return data
