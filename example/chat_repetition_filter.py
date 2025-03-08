from blossom.dataset import create_dataset

from blossom.op import ChatRepetitionFilter
from blossom.schema import ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user("1 + 1 = ?"),
            assistant("1 + 1 is equal to 2."),
        ]
    ),
    ChatSchema(
        messages=[
            user("1 + 1 = ?"),
            assistant("1 + 1 is equal to 2 to 2 to 2 to 2 to 2 to 2 to 2 to 2."),
        ]
    ),
]

ops = [
    ChatRepetitionFilter(n=10, min_ratio=0.0, max_ratio=0.5),
]

result = create_dataset(data).execute(ops).collect()
print(result)
