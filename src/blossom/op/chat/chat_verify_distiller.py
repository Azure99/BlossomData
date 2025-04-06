import re
from typing import Any, Callable, Optional, Union

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.provider.protocol import ChatCompletionFinishReason
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContent,
    ChatMessageContentText,
    ChatRole,
    user,
    assistant,
)
from blossom.schema.schema import Schema
from blossom.util.json import loads_markdown_first_json
from blossom.util.type import StrEnum

LAST_NUMBER_REGEX = r"-?\d+(?:\.\d+)?"

LLM_VERIFY_PROMPT = """For the given "Question," "Reference answer," and "Response," please analyze step by step whether the "reference answer" and "response" are consistent.

### Question:
{question}

### Reference answer:
{reference_answer}

### Response:
{model_answer}
"""

LLM_VERIFY_JSON_PROMPT = """Please output your conclusion directly in JSON format.
The JSON should contain only one boolean field named "consistent," which indicates whether the "reference answer" and the "response" are consistent.
Please output only a JSON without any explanation or other irrelevant content."""

METADATA_REASONING_COUNT = "reasoning_count"


class ChatVerifyDistiller(MapOperator):
    class Mode(StrEnum):
        NONE = "none"
        REGEX = "regex"
        LLM = "llm"
        FUNCTION = "function"

    def __init__(
        self,
        model: str,
        mode: Mode = Mode.NONE,
        reference_field: Optional[str] = None,
        validation_model: Optional[str] = None,
        validation_prompt: Optional[str] = None,
        validation_function: Optional[Callable[[str, str, str], bool]] = None,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.mode = mode
        self.reference_field = reference_field
        self.validation_model = validation_model or model
        self.validation_prompt = validation_prompt
        if self.mode == self.Mode.FUNCTION:
            assert validation_function is not None
        self.validation_function = validation_function
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        question = self._first_message_content(_item.messages, ChatRole.USER)
        reference = ""
        if self.mode != self.Mode.NONE:
            if self.reference_field:
                if self.reference_field in _item.metadata:
                    reference = _item.metadata[self.reference_field]
            if not reference:
                chat_reference = self._first_message_content(
                    _item.messages, ChatRole.ASSISTANT
                )
                assert isinstance(chat_reference, str)
                reference = chat_reference

        if not question or (not reference and self.mode != self.Mode.NONE):
            return self._cast_base(_item)

        for retry_count in range(self.max_retry):
            try:
                model_message = self._distill_with_validate(question, reference)
                _item.messages = [
                    user(question),
                    model_message,
                ]
                _item.metadata[METADATA_REASONING_COUNT] = retry_count + 1
                return self._cast_base(_item)
            except Exception as e:
                logger.info(f"Validation failed: {question}, {e}")

        _item.failed = True
        return self._cast_base(_item)

    def _distill_with_validate(
        self, question: Union[str, list[ChatMessageContent]], reference: str
    ) -> ChatMessage:
        response = self.context.chat_completion_with_details(
            model=self.model,
            messages=[user(question)],
            extra_params=self.extra_params,
        )
        finish_reason = response.choices[0].finish_reason

        # model answer is not complete
        if finish_reason != ChatCompletionFinishReason.STOP:
            # cannot validate incomplete model answer
            if self.mode != self.Mode.NONE:
                raise ValueError("Model answer is not complete")

        model_message = response.choices[0].message
        assert isinstance(model_message.content, str)
        if self._validate_model_answer(question, reference, model_message.content):
            return model_message
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

        if self.mode == self.Mode.REGEX:
            return self._validate_answer_by_regex(reference_answer, model_answer)

        if self.mode == self.Mode.LLM:
            return self._validate_answer_by_llm(
                question, reference_answer, model_answer
            )

        if self.mode == self.Mode.FUNCTION:
            if not self.validation_function:
                raise ValueError("validation_function must be provided.")
            return self.validation_function(question, reference_answer, model_answer)

        return True

    @staticmethod
    def _validate_answer_by_regex(reference_answer: str, model_answer: str) -> bool:
        matches = re.findall(LAST_NUMBER_REGEX, model_answer)
        last_number = float(matches[-1]) if matches else None
        answer_number = float(reference_answer.lower())
        return last_number == answer_number

    def _validate_answer_by_llm(
        self, question: str, reference_answer: str, model_answer: str
    ) -> bool:
        prompt_template = self.validation_prompt or LLM_VERIFY_PROMPT
        validation_prompt = prompt_template.format(
            question=question,
            reference_answer=reference_answer,
            model_answer=model_answer,
        )
        validation_messages = [user(validation_prompt)]

        validation_messages.append(
            assistant(
                self.context.chat_completion(
                    model=self.validation_model,
                    messages=validation_messages,
                )
            )
        )

        validation_messages.append(user(LLM_VERIFY_JSON_PROMPT))
        validation_json_result = self.context.chat_completion(
            model=self.validation_model, messages=validation_messages
        )
        consistent = loads_markdown_first_json(validation_json_result).get(
            "consistent", False
        )
        assert isinstance(consistent, bool)
        return consistent

    @staticmethod
    def _first_message_content(
        messages: list[ChatMessage], role: ChatRole
    ) -> Union[str, list[ChatMessageContent]]:
        return next((m.content for m in messages if m.role == role), "")
