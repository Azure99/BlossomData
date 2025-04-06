from blossom.dataset import create_dataset

from blossom.op import ChatDistiller, ChatTranslator, FailedItemFilter
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

ops = [
    ChatDistiller(model="gpt-4o-mini"),
    ChatTranslator(model="gpt-4o-mini", target_language="Chinese"),
    FailedItemFilter(),
]

result = create_dataset(data).execute(ops).collect()
print(result)
