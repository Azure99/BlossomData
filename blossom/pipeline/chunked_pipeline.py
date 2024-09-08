import json
import math
import os
import uuid
from typing import Optional

from blossom.conf import Config
from blossom.io.schema import load_schema_dict_list
from blossom.pipeline.base_pipeline import BasePipeline
from blossom.schema.base_schema import BaseSchema
from blossom.util.json import json_dumps


class ChunkedPipeline(BasePipeline):
    def __init__(
        self,
        config: Optional[Config] = None,
        chunk_size: int = 512,
        save_path: Optional[str] = None,
    ):
        super().__init__(config)
        self.chunk_size = chunk_size
        self.save_path = save_path or str(uuid.uuid4())
        self.input_path = os.path.join(self.save_path, "input")
        self.output_path = os.path.join(self.save_path, "output")
        os.makedirs(self.input_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)

    def execute(self, data: list[BaseSchema]) -> list[BaseSchema]:
        index, chunk_count = self._chunk_data(data)

        for i in range(index, chunk_count):
            self._process_chunk(i)

        result = []
        for i in range(chunk_count):
            result_file = os.path.join(self.output_path, f"{i}.json")
            with open(result_file, "r") as f:
                result.extend(load_schema_dict_list(json.loads(f.read())))
        return result

    def _process_chunk(self, index: int) -> None:
        chunk_file = os.path.join(self.input_path, f"{index}.json")
        with open(chunk_file, "r") as f:
            chunk = load_schema_dict_list(json.loads(f.read()))

        for operator in self.operators:
            chunk = operator.process(chunk)

        result_file = os.path.join(self.output_path, f"{index}.json")
        with open(result_file, "w") as f:
            f.write(json_dumps(chunk))

    def _chunk_data(self, data: list[BaseSchema]) -> tuple[int, int]:
        if os.path.exists(os.path.join(self.input_path, ".done")):
            chunk_count = len(os.listdir(self.input_path)) - 1
            next_index = len(os.listdir(self.output_path))
            return next_index, chunk_count

        if len(os.listdir(self.input_path)) > 0:
            raise ValueError(f"Input path {self.input_path} is not empty")

        for i in range(0, len(data), self.chunk_size):
            chunk = data[i : i + self.chunk_size]
            chunk_file = os.path.join(self.input_path, f"{i // self.chunk_size}.json")
            with open(chunk_file, "w") as f:
                f.write(json_dumps(chunk))

        with open(os.path.join(self.input_path, ".done"), "w") as f:
            f.write("")
        return 0, math.ceil(len(data) / self.chunk_size)
