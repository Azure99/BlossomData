from blossom.op.chat.chat_multi_reasoning_filter import ChatMultiReasoningFilter
from blossom.pipeline.simple_pipeline import SimplePipeline
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="Who developed ChatGPT?"),
            ChatMessage(role=ChatRole.ASSISTANT, content="OpenAI"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="Who developed ChatGPT?"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Google"),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatMultiReasoningFilter(review_model="gpt-4o-mini", reasoning_model="gpt-4o-mini"),
)

result = pipeline.execute(data)
print(result)
