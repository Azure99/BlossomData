from blossom.op import ChatContentFilter
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


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
