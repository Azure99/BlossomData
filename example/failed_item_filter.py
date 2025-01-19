from blossom.op import ChatDistill, ChatTranslate, FailedItemFilter
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user("hello"),
        ]
    ),
    ChatSchema(
        messages=[
            assistant("hello"),
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
