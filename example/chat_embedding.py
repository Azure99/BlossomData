from blossom.dataset import create_dataset

from blossom.op import ChatEmbedding
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

ops = [
    ChatEmbedding(model="text-embedding-3-small"),
]

result = create_dataset(data).execute(ops).collect()
print(result)
