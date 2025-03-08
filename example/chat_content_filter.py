from blossom.dataset import create_dataset

from blossom.op import ChatContentFilter
from blossom.schema import ChatRole, ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user("who developed you"),
            assistant("OpenAI developed me"),
        ]
    ),
    ChatSchema(
        messages=[
            user("who developed you"),
            assistant("Google developed me"),
        ]
    ),
]

ops = [
    ChatContentFilter(
        contents=["openai"], roles=[ChatRole.ASSISTANT], case_sensitive=False
    ),
]

result = create_dataset(data).execute(ops).collect()
print(result)
