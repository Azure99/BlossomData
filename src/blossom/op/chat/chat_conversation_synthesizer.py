import json
from typing import Any, Callable, Optional

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.provider.protocol import ChatCompletionFinishReason
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContent,
    ChatMessageContentText,
    ChatRole,
    user,
)
from blossom.schema.schema import Schema
from blossom.util.json import loads_markdown_first_json

USER_SIMULATOR_PROMPT = """You are a "simulated user follow-up question generator".

Your task:
- Input: A piece of conversation history about some topic, in the form of a JSON array whose elements are alternating user / assistant messages.
- Output: A JSON object containing two fields:
  1. "analysis": your brief analysis of this conversation and of the user.
  2. "next_user_question": the new question that the same user is likely to ask in the next turn after seeing the current answer (simulating a human input style).

-------------------------------
[Input format]

You will receive a JSON array. Its structure is as follows:

[
  {
    "role": "user",
    "content": "..."
  },
  {
    "role": "assistant",
    "content": "..."
  },
  {
    "role": "user",
    "content": "..."
  },
  {
    "role": "assistant",
    "content": "..."
  }
]

Conventions:
- The value of "role" is always either "user" or "assistant".
- All "user" messages in the array come from the same real user (the same person).
- The array length may be 2 (single-turn) or longer (multi-turn).
- It usually ends with an "assistant" message; in that case you must generate the “next-round user question” for this history.

You need to:
1. Read the entire array carefully and understand the context.
2. Pay special attention to the most recent "user" messages in order to infer the user's speaking style, knowledge level, and true needs.
3. Based on the content of the latest "assistant" answer, think about what the user would most naturally ask next.

-------------------------------
[Requirements for the analysis field]

Before generating the new question, first perform a brief analysis and write the result into the "analysis" field. The content should include, but is not limited to:

- A rough user profile: for example, more of a beginner vs. more professional; what type of problem they are trying to solve (studying, coding, work, life advice, etc.).
- The topic and core goal of the current conversation.
- Which important questions have not been fully resolved in the answer? (List 1-3 possible follow-up directions.)
- Which follow-up direction you finally choose, and why this direction is the most natural / valuable.

-------------------------------
[Requirements for generating the next_user_question field]

In the "next_user_question" field, write the “next-turn question” that the same user might ask.

It must satisfy the following:

1. Thematic coherence
   - The new question must be a natural continuation or deepening of the current conversation:
     - It can ask for more detail (for example: "So how do I do this concretely?")
     - It can ask for more examples / cases.
     - It can ask to compare pros and cons of different solutions.
     - It can ask about edge cases, exceptions, or practical implementation.
   - Do not jump to a completely unrelated new topic.

2. Consistent style
   - Imitate the user's original way of speaking and input habits as much as possible:
     - Tone (polite vs. direct; formal vs. colloquial).
     - Sentence length; whether they like to use lists / multiple clauses.
     - Whether they use emojis and filler particles.
     - What language(s) they use (Chinese, English, mixed, etc.); prefer the language the user originally used.
   - If the style of user messages varies slightly over multiple turns, regard the most recent 1-2 user messages as primary.

3. Simulate real human input (light noise)
   - To make it more realistic, you may add a small amount of natural “input noise”:
     - Slightly irregular punctuation: e.g., overusing commas / ellipses, multiple question or exclamation marks (such as "???", "...?", etc.).
     - Moderate colloquial expressions: e.g., "I'm kind of confused", "what should I do", "this feels a bit weird", "is there some other way to do this", etc.
     - A few minor typos or slight grammatical issues, but the text must remain easy to understand and must not significantly hinder readability.
   - The noise should be "slight and natural"; do not deliberately create many errors or any unreadable, jumbled text.

4. Matching length and difficulty
   - The length of the new question should be roughly similar to the user's original question (it can be slightly longer), but should not suddenly turn into an essay-length query.
   - The depth of knowledge should match the user's level:
     - If the user seems to be a beginner, do not suddenly produce very professional or obscure questions.
     - If the user appears very professional, you may go a bit deeper.

5. Ask only, do not answer
   - In "next_user_question" include only the question itself.
   - Do not include any explanations, background descriptions, or leading phrases (for example, phrases like "As my next question I would like to ask:" must not appear).
   - Do not include any "assistant"-style voice or answer content.

-------------------------------
[Output format]

Your final output must be a valid JSON object, and may contain only the following two fields:

- "analysis": string
- "next_user_question": string

Example of the output structure (structure only; actual content will differ):

{
  "analysis": "...this is your brief analysis of the user and the conversation...",
  "next_user_question": "...this is the simulated user's new question for the next turn..."
}

Formatting requirements:
- Do not output any additional text outside the JSON (such as "Okay, here are the results:", etc.).
- Do not add inline comments or extra fields.
- Keys must use double quotes, and string values must be valid JSON strings.
- Ensure that the entire reply can be directly parsed by a JSON parser.

-------------------------------
[Conversation JSON to process]

{CONVERSATION_JSON}
""".strip()

METADATA_RESPONSE_TRUNCATED = "response_truncated"


class ChatMultiTurnSynthesizer(MapOperator):
    """Multi-turn conversation synthesizer for ChatSchema."""

    def __init__(
        self,
        model: str,
        user_simulator_model: Optional[str] = None,
        input_truncate_rounds: Optional[int] = 1,
        max_total_tokens: Optional[int] = None,
        max_rounds: Optional[int] = None,
        max_rounds_func: Optional[Callable[[Schema], int]] = None,
        user_simulator_prompt: str = USER_SIMULATOR_PROMPT,
        max_retry: int = 1,
        assistant_extra_params: Optional[dict[str, Any]] = None,
        user_simulator_extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        """
        Initialize the synthesizer.

        Args:
            model: Model used to generate assistant replies.
            user_simulator_model: Model used to generate next user questions;
                falls back to `model` when None.
            input_truncate_rounds: Optional number of existing assistant rounds
                to keep before synthesis. None means no truncation.
            max_total_tokens: Optional upper bound on total tokens for each
                assistant generation. None disables this limit.
            max_rounds: Maximum total assistant turns (including existing ones)
                allowed in the conversation. None allows using max_rounds_func.
            max_rounds_func: Optional function that receives a sample item and
                returns max_rounds for that sample.
            user_simulator_prompt: Prompt template for the user simulator.
            max_retry: Maximum times to retry synthesis for a single item.
            assistant_extra_params: Extra params forwarded to the assistant model.
            user_simulator_extra_params: Extra params forwarded to the
                user-simulator model.
            parallel: Number of parallel worker threads.
        """
        super().__init__(parallel=parallel)
        self.model = model
        self.user_simulator_model = user_simulator_model
        if input_truncate_rounds is not None and input_truncate_rounds < 1:
            raise ValueError("input_truncate_rounds must be at least 1")
        self.input_truncate_rounds = input_truncate_rounds
        if max_total_tokens is not None and max_total_tokens <= 0:
            raise ValueError("max_total_tokens must be greater than 0")
        self.max_total_tokens = max_total_tokens
        if max_rounds is None and max_rounds_func is None:
            raise ValueError("max_rounds and max_rounds_func cannot both be None")
        if max_rounds_func is not None and not callable(max_rounds_func):
            raise ValueError("max_rounds_func must be callable")
        if max_rounds is not None and max_rounds < 1:
            raise ValueError("max_rounds must be at least 1")
        self.max_rounds = max_rounds
        self.max_rounds_func = max_rounds_func
        self.user_simulator_prompt = user_simulator_prompt
        self.max_retry = max_retry
        self.assistant_extra_params = assistant_extra_params
        self.user_simulator_extra_params = user_simulator_extra_params

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        new_messages: list[ChatMessage] = []
        last_exception: Optional[Exception] = None

        for _ in range(self.max_retry):
            try:
                max_rounds = self._resolve_max_rounds(_item)
                new_messages, truncated = self._synthesize_conversation(
                    _item.messages, max_rounds
                )
                if truncated:
                    _item.metadata[METADATA_RESPONSE_TRUNCATED] = True

                break
            except Exception as e:
                last_exception = e
                logger.exception(f"Failed to synthesize chat conversation: {e}")

        if new_messages:
            _item.messages = new_messages
        else:
            _item.mark_failed(
                str(last_exception) if last_exception else "Failed to synthesize chat"
            )

        return self._cast_base(_item)

    def _synthesize_conversation(
        self, messages: list[ChatMessage], max_rounds: int
    ) -> tuple[list[ChatMessage], bool]:
        messages = self._truncate_input_messages(messages)

        assistant_rounds = sum(
            1 for message in messages if message.role == ChatRole.ASSISTANT
        )

        while assistant_rounds < max_rounds:
            # If the latest message is assistant, generate next user question.
            if messages and messages[-1].role == ChatRole.ASSISTANT:
                next_question = self._generate_next_user_question(messages)
                messages.append(ChatMessage(role=ChatRole.USER, content=next_question))

            response = self.context.chat_completion_with_details(
                model=self.model,
                messages=messages,
                extra_params=self.assistant_extra_params,
            )

            choice = response.choices[0]
            messages.append(choice.message)

            assistant_rounds += 1

            if choice.finish_reason == ChatCompletionFinishReason.LENGTH:
                return messages, True

            if self.max_total_tokens and (
                response.usage.total_tokens >= self.max_total_tokens
            ):
                break

        return messages, False

    def _resolve_max_rounds(self, item: Schema) -> int:
        if self.max_rounds_func is not None:
            max_rounds = self.max_rounds_func(item)
            if not isinstance(max_rounds, int):
                raise ValueError("max_rounds_func must return an int")
            if max_rounds < 1:
                raise ValueError("max_rounds_func must return at least 1")
            return max_rounds
        if self.max_rounds is None:
            raise ValueError("max_rounds is required when max_rounds_func is None")
        return self.max_rounds

    def _truncate_input_messages(
        self, messages: list[ChatMessage]
    ) -> list[ChatMessage]:
        if self.input_truncate_rounds is None:
            return list(messages)

        assistant_count = 0
        cut_index = len(messages)

        for index, message in enumerate(messages):
            if message.role == ChatRole.ASSISTANT:
                assistant_count += 1
                if assistant_count == self.input_truncate_rounds:
                    cut_index = index + 1
                elif assistant_count > self.input_truncate_rounds:
                    break

        if assistant_count >= self.input_truncate_rounds:
            return list(messages[:cut_index])
        return list(messages)

    def _generate_next_user_question(self, messages: list[ChatMessage]) -> str:
        model_name = self.user_simulator_model or self.model
        conversation_json = self._build_conversation_json(messages)
        prompt = self.user_simulator_prompt.replace(
            "{CONVERSATION_JSON}", conversation_json
        )

        raw_result = self.context.chat_completion(
            model=model_name,
            messages=[user(prompt)],
            extra_params=self.user_simulator_extra_params,
        )

        try:
            result = loads_markdown_first_json(raw_result)
        except Exception as e:
            logger.exception(f"Failed to parse user simulator output as JSON: {e}")
            raise

        next_question = result.get("next_user_question", "")
        if not isinstance(next_question, str):
            raise ValueError("next_user_question must be a string in simulator output")

        next_question = next_question.strip()
        assert next_question, "next_user_question cannot be empty"
        return next_question

    @staticmethod
    def _build_conversation_json(messages: list[ChatMessage]) -> str:
        simplified_messages: list[dict[str, str]] = []

        for message in messages:
            if message.role not in {ChatRole.USER, ChatRole.ASSISTANT}:
                continue
            simplified_messages.append(
                {
                    "role": message.role.value,
                    "content": ChatMultiTurnSynthesizer._message_to_text(
                        message.content
                    ),
                }
            )

        return json.dumps(simplified_messages, ensure_ascii=False)

    @staticmethod
    def _message_to_text(content: Any) -> str:
        if isinstance(content, str):
            return content

        if isinstance(content, list):
            parts: list[str] = []
            for part in content:
                if isinstance(part, ChatMessageContentText):
                    parts.append(part.text)
                elif isinstance(part, ChatMessageContent):
                    parts.append("[non-text content]")
            return " ".join(parts).strip()

        return str(content)
