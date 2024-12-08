import re
from enum import Enum
from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatMessage, ChatRole
from blossom.util.json import loads_markdown_first_json

LAST_NUMBER_REGEX = r"-?\d+(?:\.\d+)?"

LLM_CHECK_PROMPT = """For the given "Question," "Reference answer," and "Response," please analyze step by step whether the "reference answer" and "response" are consistent.

### Question:
{question}

### Reference answer:
{reference_answer}

### Response:
{model_answer}
"""

LLM_CHECK_JSON_PROMPT = """Please output your conclusion directly in JSON format.
The JSON should contain only one boolean field named "consistent," which indicates whether the "reference answer" and the "response" are consistent.
Please output only a JSON without any explanation or other irrelevant content."""


class ChatMathDistill(MapOperator):
    class ValidateMode(Enum):
        NONE = 0
        REGEX = 1
        LLM = 2

    def __init__(
        self,
        model: str,
        validate_mode: ValidateMode = ValidateMode.NONE,
        validate_model: Optional[str] = None,
        reference_field: Optional[str] = None,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.validate_mode = validate_mode
        self.validate_model = validate_model or model
        self.reference_field = reference_field
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        question = self._first_message_content(_item.messages, ChatRole.USER)
        reference = ""
        if self.validate_mode != self.ValidateMode.NONE:
            if self.reference_field:
                if self.reference_field in _item.metadata:
                    reference = _item.metadata[self.reference_field]
            if not reference:
                reference = self._first_message_content(
                    _item.messages, ChatRole.ASSISTANT
                )

        _item.messages = []

        if not question or (
            not reference and self.validate_mode != self.ValidateMode.NONE
        ):
            return self._cast_base(_item)

        for _ in range(self.max_retry):
            try:
                model_answer = self._distill_with_validate(question, reference)
                _item.messages.append(ChatMessage(role=ChatRole.USER, content=question))
                _item.messages.append(
                    ChatMessage(role=ChatRole.ASSISTANT, content=model_answer)
                )
                return self._cast_base(_item)
            except Exception:
                pass

        return self._cast_base(_item)

    def _distill_with_validate(self, question: str, reference: str) -> str:
        model_answer = self.context.single_chat_completion(
            model=self.model,
            user_message=question,
            extra_params=self.extra_params,
        )
        if self._validate_model_answer(question, reference, model_answer):
            return model_answer
        raise ValueError("Model answer is not consistent with the reference answer")

    def _validate_model_answer(
        self, question: str, reference_answer: str, model_answer: str
    ) -> bool:
        if self.validate_mode == self.ValidateMode.REGEX:
            return self._validate_model_answer_regex(reference_answer, model_answer)

        if self.validate_mode == self.ValidateMode.LLM:
            return self._validate_model_answer_llm(
                question, reference_answer, model_answer
            )

        return True

    def _validate_model_answer_regex(
        self, reference_answer: str, model_answer: str
    ) -> bool:
        matches = re.findall(LAST_NUMBER_REGEX, model_answer)
        last_number = float(matches[-1]) if matches else None
        answer_number = float(reference_answer.lower())
        return last_number == answer_number

    def _validate_model_answer_llm(
        self, question: str, reference_answer: str, model_answer: str
    ) -> bool:
        validate_prompt = LLM_CHECK_PROMPT.format(
            question=question,
            reference_answer=reference_answer,
            model_answer=model_answer,
        )
        validate_messages = self.context.chat_completion_with_messages(
            model=self.validate_model,
            messages=[ChatMessage(role=ChatRole.USER, content=validate_prompt)],
        )

        validate_messages.append(
            ChatMessage(role=ChatRole.USER, content=LLM_CHECK_JSON_PROMPT)
        )
        validate_json_result = self.context.chat_completion(
            model=self.validate_model, messages=validate_messages
        )
        return loads_markdown_first_json(validate_json_result).get("consistent", False)

    @staticmethod
    def _first_message_content(messages: list[ChatMessage], role: ChatRole) -> str:
        return next((m.content for m in messages if m.role == role), "")
