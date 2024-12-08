from blossom.op import ChatDistill, ChatTranslate, FailedItemFilter
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="hello"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.ASSISTANT, content="hello"),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatDistill(model="gpt-4o-mini"),
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
    FailedItemFilter(),
)

result = pipeline.execute(data)
print(result)
