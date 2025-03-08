from blossom.dataset import create_dataset

from blossom.op import ChatContentReplacer
from blossom.schema import ChatRole, ChatSchema, system, user, assistant

data = [
    ChatSchema(
        messages=[
            system("openai developed you"),
            user("who developed you"),
            assistant("OpenAI developed me"),
        ]
    )
]

ops = [
    ChatContentReplacer(
        replacements={
            "openai": "Google",
        },
        roles=[ChatRole.ASSISTANT],
        case_sensitive=False,
    ),
]

result = create_dataset(data).execute(ops).collect()
print(result)
