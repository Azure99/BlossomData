from blossom.op import ChatEmbedding
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user("How was dinner today? Was it tasty?"),
            assistant("It was so tasty, thank you!"),
        ]
    ),
    ChatSchema(
        messages=[
            user("Was dinner delicious today?"),
            assistant("Yes, it was delicious!"),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatEmbedding(model="text-embedding-3-small"),
)

result = pipeline.execute(data)
print(result)
