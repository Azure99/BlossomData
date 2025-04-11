import re
from blossom.dataset import create_dataset

from blossom.op import ChatLengthFilter
from blossom.schema import ChatSchema, user, assistant


def count_words(text):
    tokens = re.findall(r"\w+|[^\w\s]", text)
    return len(tokens)


data = [
    ChatSchema(
        messages=[
            user("hello world"),
            assistant("hello world hello world"),
        ]
    ),
    ChatSchema(
        messages=[
            user("hello world"),
            assistant("hello world hello world hello world hello world hello world"),
        ]
    ),
]

ops = [
    ChatLengthFilter(len_func=count_words, assistant_max_len=9),
]

result = create_dataset(data).execute(ops).collect()
print(result)
