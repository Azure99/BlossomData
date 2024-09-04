from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema


class ChatMultiReasoningFilter(FilterOperator):
    def __init__(
        self,
        review_model: str,
        reasoning_models: list[str],
        skip_subjective: bool = True,
    ):
        super().__init__()
        self.review_model = review_model
        self.reasoning_models = reasoning_models
        self.skip_subjective = skip_subjective

    def process_item(self, item: BaseSchema) -> bool:
        raise NotImplementedError("Operator not implemented")
