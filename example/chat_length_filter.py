from blossom.dataset import create_dataset

from blossom.op import ChatLengthFilter
from blossom.schema import ChatSchema, user, assistant

tokenizer = None


def llama_tokenizer_len(text):
    global tokenizer
    from transformers import AutoTokenizer

    if not tokenizer:
        tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen2.5-1.5B")
    length = len(tokenizer.encode(text))
    print(f"'{text}' tokenized length: {length}")
    return length


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
    ChatLengthFilter(len_func=llama_tokenizer_len, assistant_max_len=9),
]

result = create_dataset(data).execute(ops).collect()
print(result)
