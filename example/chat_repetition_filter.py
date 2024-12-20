from blossom.op import ChatRepetitionFilter
from blossom.pipeline import SimplePipeline
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

pipeline = SimplePipeline().add_operators(
    ChatRepetitionFilter(n=10, min_ratio=0.0, max_ratio=0.5),
)

result = pipeline.execute(data)
print(result)
