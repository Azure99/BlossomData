from blossom.op.chat.chat_content_filter import ChatContentFilter
from blossom.pipeline.simple_pipeline import SimplePipeline
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="who developed you"),
            ChatMessage(role=ChatRole.ASSISTANT, content="OpenAI developed me"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="who developed you"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Google developed me"),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatContentFilter(
        contents=["openai"], roles=[ChatRole.ASSISTANT], case_sensitive=False
    ),
)

result = pipeline.execute(data)
print(result)
