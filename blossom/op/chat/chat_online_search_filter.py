from enum import Enum

from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema


class ChatOnlineSearchFilter(FilterOperator):
    class Engine(Enum):
        DUCK_DUCK_GO = 0
        GOOGLE = 1

    def __init__(
        self,
        review_model: str,
        search_engine: Engine,
        skip_subjective: bool = True,
        reverse: bool = False,
        parallel: int = 1,
    ):
        super().__init__(reverse=reverse, parallel=parallel)
        self.review_model = review_model
        self.search_engine = search_engine
        self.skip_subjective = skip_subjective

    def process_item(self, item: BaseSchema) -> bool:
        raise NotImplementedError("Operator not implemented")
