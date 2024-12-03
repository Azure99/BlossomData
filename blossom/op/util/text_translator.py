import json
from typing import Any, Optional

from blossom.provider.base_provider import BaseProvider
from blossom.schema.chat_schema import ChatMessage, ChatRole
from blossom.util.json import extract_markdown_first_json, json_dumps

TRANSLATE_PROMPT_TEMPLATE = """Your task is to translate the text into {target_language}. When translating, you need to follow the following rules:
1. The text to be translated is the text field in json.
2. The output also needs to be in json format, and the translated text is in the result field.
3. The translated text must conform to {target_language} grammar and {target_language} expression habits.
{part_instruction_only}

{json}
"""

TRANSLATE_PROMPT_PART_INSTRUCTION_ONLY = '4. Just translate the instructions in the text and output "code, script, data," etc., as is without translating them.'


class TextTranslator:
    def __init__(self, provider: BaseProvider) -> None:
        self.provider = provider

    def translate(
        self,
        content: str,
        target_language: str,
        instruction_only: bool,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> str:
        prompt = TRANSLATE_PROMPT_TEMPLATE.format(
            target_language=target_language,
            part_instruction_only=(
                TRANSLATE_PROMPT_PART_INSTRUCTION_ONLY if instruction_only else ""
            ),
            json=json_dumps({"text": content}),
        )

        last_exception = None
        for _ in range(max_retry):
            try:
                return self._translate_and_extract(prompt, extra_params)
            except Exception as e:
                last_exception = e

        raise ValueError("Failed to translate text") from last_exception

    def _translate_and_extract(
        self, prompt: str, extra_params: Optional[dict[str, Any]] = None
    ) -> str:
        translate_output = self.provider.chat_completion(
            messages=[ChatMessage(role=ChatRole.USER, content=prompt)],
            extra_params=extra_params,
        )
        json_output = extract_markdown_first_json(translate_output)
        result = json.loads(json_output)["result"]
        if not isinstance(result, str):
            raise ValueError("Failed to extract translation result")
        return result
