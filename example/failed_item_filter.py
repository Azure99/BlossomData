from blossom.dataset import create_dataset

from blossom.op import ChatDistill, ChatTranslate, FailedItemFilter
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
    ChatDistill(model="gpt-4o-mini"),
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
    FailedItemFilter(),
]

result = create_dataset(data).execute(ops).collect()
print(result)
