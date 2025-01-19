import re
from enum import Enum
from typing import Any, Optional, Union

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.provider.protocol import ChatCompletionFinishReason
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContent,
    ChatMessageContentText,
    ChatRole,
    user,
    assistant,
)
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

METADATA_REASONING_COUNT = "reasoning_count"


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
        check_prompt: Optional[str] = None,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.validate_mode = validate_mode
        self.validate_model = validate_model or model
        self.reference_field = reference_field
        self.check_prompt = check_prompt
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
                chat_reference = self._first_message_content(
                    _item.messages, ChatRole.ASSISTANT
                )
                assert isinstance(chat_reference, str)
                reference = chat_reference

        if not question or (
            not reference and self.validate_mode != self.ValidateMode.NONE
        ):
            return self._cast_base(_item)

        for retry_count in range(self.max_retry):
            try:
                model_answer = self._distill_with_validate(question, reference)
                _item.messages = [
                    user(question),
                    assistant(model_answer),
                ]
                _item.metadata[METADATA_REASONING_COUNT] = retry_count + 1
                return self._cast_base(_item)
            except Exception as e:
                logger.info(f"Validation failed: {question}, {e}")

        _item.failed = True
        return self._cast_base(_item)

    def _distill_with_validate(
        self, question: Union[str, list[ChatMessageContent]], reference: str
    ) -> str:
        response = self.context.chat_completion_with_details(
            model=self.model,
            messages=[user(question)],
            extra_params=self.extra_params,
        )
        finish_reason = response.choices[0].finish_reason

        # model answer is not complete
        if finish_reason != ChatCompletionFinishReason.STOP:
            # cannot validate incomplete model answer
            if self.validate_mode != self.ValidateMode.NONE:
                raise ValueError("Model answer is not complete")

        model_answer = response.choices[0].message.content
        assert isinstance(model_answer, str)
        if self._validate_model_answer(question, reference, model_answer):
            return model_answer
        raise ValueError("Model answer is not consistent with the reference answer")

    def _validate_model_answer(
        self,
        question: Union[str, list[ChatMessageContent]],
        reference_answer: str,
        model_answer: str,
    ) -> bool:
        if isinstance(question, list):
            for part in question:
                if isinstance(part, ChatMessageContentText):
                    question = part.text
                    break
        assert isinstance(question, str)

        if self.validate_mode == self.ValidateMode.REGEX:
            return self._validate_model_answer_regex(reference_answer, model_answer)

        if self.validate_mode == self.ValidateMode.LLM:
            return self._validate_model_answer_llm(
                question, reference_answer, model_answer
            )

        return True

    @staticmethod
    def _validate_model_answer_regex(reference_answer: str, model_answer: str) -> bool:
        matches = re.findall(LAST_NUMBER_REGEX, model_answer)
        last_number = float(matches[-1]) if matches else None
        answer_number = float(reference_answer.lower())
        return last_number == answer_number

    def _validate_model_answer_llm(
        self, question: str, reference_answer: str, model_answer: str
    ) -> bool:
        prompt_template = self.check_prompt or LLM_CHECK_PROMPT
        validate_prompt = prompt_template.format(
            question=question,
            reference_answer=reference_answer,
            model_answer=model_answer,
        )
        validate_messages = [user(validate_prompt)]

        validate_messages.append(
            assistant(
                self.context.chat_completion(
                    model=self.validate_model,
                    messages=validate_messages,
                )
            )
        )

        validate_messages.append(user(LLM_CHECK_JSON_PROMPT))
        validate_json_result = self.context.chat_completion(
            model=self.validate_model, messages=validate_messages
        )
        consistent = loads_markdown_first_json(validate_json_result).get(
            "consistent", False
        )
        assert isinstance(consistent, bool)
        return consistent

    @staticmethod
    def _first_message_content(
        messages: list[ChatMessage], role: ChatRole
    ) -> Union[str, list[ChatMessageContent]]:
        return next((m.content for m in messages if m.role == role), "")
