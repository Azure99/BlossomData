from typing import Optional

from blossom.conf.config import Config
from blossom.pipeline.base_pipeline import BasePipeline
from blossom.schema.base_schema import BaseSchema


class SimplePipeline(BasePipeline):
    def __init__(self, config: Optional[Config] = None):
        super().__init__(config)

    def execute(self, data: list[BaseSchema]) -> list[BaseSchema]:
        for operator in self.operators:
            data = operator.process(data)
        return data
