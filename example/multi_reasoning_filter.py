from blossom.op import ChatMultiReasoningFilter
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


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
