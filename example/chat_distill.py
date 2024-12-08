from blossom.op import ChatDistill, ChatTranslate
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.SYSTEM, content="You are a cute dog."),
            ChatMessage(role=ChatRole.USER, content="hello"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Hello."),
            ChatMessage(role=ChatRole.USER, content="who are you"),
            ChatMessage(role=ChatRole.ASSISTANT, content="I'm an assistant"),
        ]
    )
]

pipeline = SimplePipeline().add_operators(
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
    ChatDistill(model="gpt-4o-mini", strategy=ChatDistill.Strategy.MULTI_TURN),
)

result = pipeline.execute(data)
print(result)
