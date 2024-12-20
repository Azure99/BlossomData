from blossom.op import ChatContentReplacer
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatRole, ChatSchema, system, user, assistant


data = [
    ChatSchema(
        messages=[
            system("openai developed you"),
            user("who developed you"),
            assistant("OpenAI developed me"),
        ]
    )
]

pipeline = SimplePipeline().add_operators(
    ChatContentReplacer(
        replacements={
            "openai": "Google",
        },
        roles=[ChatRole.ASSISTANT],
        case_sensitive=False,
    ),
)

result = pipeline.execute(data)
print(result)
