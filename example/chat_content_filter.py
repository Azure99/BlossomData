from blossom.op import ChatContentFilter
from blossom.pipeline import SimplePipeline
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

pipeline = SimplePipeline().add_operators(
    ChatContentFilter(
        contents=["openai"], roles=[ChatRole.ASSISTANT], case_sensitive=False
    ),
)

result = pipeline.execute(data)
print(result)
