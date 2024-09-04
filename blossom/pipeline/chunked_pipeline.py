from typing import Optional

from blossom.conf import Config
from blossom.pipeline.base_pipeline import BasePipeline
from blossom.schema.base_schema import BaseSchema


class ChunkedPipeline(BasePipeline):
    def __init__(self, config: Optional[Config] = None, chunk_size: int = 512):
        super().__init__(config)

    def execute(self, data: list[BaseSchema]) -> list[BaseSchema]:
        raise NotImplementedError("Pipeline not implemented")
