from blossom.op import ChatMultiReasoningFilter
from blossom.pipeline import SimplePipeline
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

pipeline = SimplePipeline().add_operators(
    ChatMultiReasoningFilter(review_model="gpt-4o-mini", reasoning_model="gpt-4o-mini"),
)

result = pipeline.execute(data)
print(result)
