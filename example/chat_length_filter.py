from blossom.op import ChatLengthFilter
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema

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
            ChatMessage(role=ChatRole.USER, content="hello world"),
            ChatMessage(role=ChatRole.ASSISTANT, content="hello world hello world"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="hello world"),
            ChatMessage(
                role=ChatRole.ASSISTANT,
                content="hello world hello world hello world hello world hello world",
            ),
        ]
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatLengthFilter(len_func=llama_tokenizer_len, assistant_max_len=9),
)

result = pipeline.execute(data)
print(result)
