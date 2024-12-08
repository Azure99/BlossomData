from blossom.op import ChatEmbedding
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.USER, content="How was dinner today? Was it tasty?"
            ),
            ChatMessage(role=ChatRole.ASSISTANT, content="It was so tasty, thank you!"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="Was dinner delicious today?"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Yes, it was delicious!"),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatEmbedding(model="text-embedding-3-small"),
)

result = pipeline.execute(data)
print(result)
