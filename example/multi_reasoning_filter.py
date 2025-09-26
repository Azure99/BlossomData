from blossom.dataset import create_dataset

from blossom.op import ChatReasoningConsistencyFilter
from blossom.schema import ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user("Who developed ChatGPT?"),
            assistant("OpenAI"),
        ]
    ),
    ChatSchema(
        messages=[
            user("Who developed ChatGPT?"),
            assistant("Google"),
        ]
    ),
]

ops = [
    ChatReasoningConsistencyFilter(
        review_model="gpt-4o-mini", reasoning_model="gpt-4o-mini"
    ),
]

result = create_dataset(data).execute(ops).collect()
print(result)
