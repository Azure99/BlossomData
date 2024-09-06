from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema
from blossom.util.json import loads_markdown_first_json


LLM_CHECK_PROMPT = """For the given "Question," "Response_1" and "Response_2" please analyze step by step whether the answers of  "Response_1" and "Response_2" are obviously inconsistent.

### Question:
{question}

### Response_1:
{response_1}

### Response_2:
{response_2}
"""

LLM_CHECK_JSON_PROMPT = """Please output your conclusion directly in JSON format.
The JSON should contain only one boolean field named "inconsistent," which indicates whether the "reference answer" and the "response" are obviously inconsistent.
Please output only a JSON without any explanation or other irrelevant content."""

class ChatMultiReasoningFilter(FilterOperator):
    def __init__(
        self,
        review_model: str,
        reasoning_model: str,
        max_retry: int = 1,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.review_model = review_model
        self.reasoning_model = reasoning_model
        self.max_retry = max_retry

    def process_item(self, item: BaseSchema) -> bool:
        _item = self._cast_chat(item)

        for _ in range(self.max_retry):
            try:
                return self._process_item(_item)
            except Exception:
                pass
        
        return False
    
    def _process_item(self, item: ChatSchema) -> bool:
        question = item.messages[-2].content
        response1 = item.messages[-1].content
        response2 = self.context.chat_completion(model=self.reasoning_model, messages=item.messages[:-1])

        validate_prompt = LLM_CHECK_PROMPT.format(
            question=question,
            response_1=response1,
            response_2=response2,
        )
        validate_messages = [ChatMessage(role=ChatRole.USER, content=validate_prompt)]
        validate_result = self.context.chat_completion(
            model=self.review_model, messages=validate_messages
        )
        validate_messages.extend(
            [
                ChatMessage(role=ChatRole.ASSISTANT, content=validate_result),
                ChatMessage(role=ChatRole.USER, content=LLM_CHECK_JSON_PROMPT),
            ]
        )
        validate_json_result = self.context.chat_completion(
            model=self.review_model, messages=validate_messages
        )
        inconsistent = loads_markdown_first_json(validate_json_result).get("inconsistent", False)
        consistent = not inconsistent
        return consistent

