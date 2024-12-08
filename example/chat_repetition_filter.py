from blossom.op import ChatRepetitionFilter
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema

data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="1 + 1 = ?"),
            ChatMessage(role=ChatRole.ASSISTANT, content="1 + 1 is equal to 2."),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="1 + 1 = ?"),
            ChatMessage(
                role=ChatRole.ASSISTANT,
                content="1 + 1 is equal to 2 to 2 to 2 to 2 to 2 to 2 to 2 to 2.",
            ),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatRepetitionFilter(n=10, min_ratio=0.0, max_ratio=0.5),
)

result = pipeline.execute(data)
print(result)
