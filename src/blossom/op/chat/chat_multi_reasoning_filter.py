from typing import Optional

from blossom.log import logger
from blossom.op.filter_operator import FilterOperator
from blossom.schema.chat_schema import ChatSchema, assistant, user
from blossom.schema.schema import Schema
from blossom.util.json import loads_markdown_first_json

LLM_VERIFY_PROMPT = """For the given "Question," "Response_1" and "Response_2" please analyze step by step whether the answers of  "Response_1" and "Response_2" are obviously inconsistent.

### Question:
{question}

### Response_1:
{response_1}

### Response_2:
{response_2}
"""

LLM_VERIFY_JSON_PROMPT = """Please output your conclusion directly in JSON format.
The JSON should contain only one boolean field named "inconsistent," which indicates whether the "reference answer" and the "response" are obviously inconsistent.
Please output only a JSON without any explanation or other irrelevant content."""


class ChatMultiReasoningFilter(FilterOperator):
    def __init__(
        self,
        review_model: str,
        reasoning_model: Optional[str] = None,
        reference_field: Optional[str] = None,
        validation_prompt: Optional[str] = None,
        max_retry: int = 1,
        reverse: bool = False,
        parallel: int = 1,
    ):
        super().__init__(reverse=reverse, parallel=parallel)
        self.review_model = review_model
        self.reasoning_model = reasoning_model
        self.reference_field = reference_field
        self.validation_prompt = validation_prompt
        self.max_retry = max_retry
        if self.reasoning_model is None and self.reference_field is None:
            raise ValueError("reasoning_model or reference_field must be provided")

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_chat(item)

        for _ in range(self.max_retry):
            try:
                return self._process_item(_item)
            except Exception as e:
                logger.info(f"Failed to reason or validate chat: {e}")

        item.failed = True
        return True

    def _process_item(self, item: ChatSchema) -> bool:
        question = item.messages[-2].content
        model_answer = item.messages[-1].content

        reference = None
        if self.reference_field:
            reference = item.metadata.get(self.reference_field)

        if not reference and self.reasoning_model:
            reference = self.context.chat_completion(
                model=self.reasoning_model, messages=item.messages[:-1]
            )

        if not reference:
            return False

        validation_prompt = self.validation_prompt or LLM_VERIFY_PROMPT
        validation_messages = [
            user(
                validation_prompt.format(
                    question=question, response_1=model_answer, response_2=reference
                )
            )
        ]
        validation_messages.append(
            assistant(
                self.context.chat_completion(
                    model=self.review_model, messages=validation_messages
                )
            )
        )
        validation_messages.append(user(LLM_VERIFY_JSON_PROMPT))

        validation_json_result = self.context.chat_completion(
            model=self.review_model, messages=validation_messages
        )
        inconsistent = loads_markdown_first_json(validation_json_result).get(
            "inconsistent", False
        )
        consistent = not inconsistent
        return consistent
