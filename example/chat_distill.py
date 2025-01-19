from blossom.op import ChatDistill, ChatTranslate
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatSchema, system, user, assistant

data = [
    ChatSchema(
        messages=[
            system("You are a cute dog."),
            user("hello"),
            assistant("Hello."),
            user("who are you"),
            assistant("I'm an assistant"),
        ]
    )
]

pipeline = SimplePipeline().add_operators(
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
    ChatDistill(model="gpt-4o-mini", strategy=ChatDistill.Strategy.MULTI_TURN),
)

result = pipeline.execute(data)
print(result)
