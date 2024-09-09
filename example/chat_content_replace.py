from blossom.op.chat.chat_content_replacer import ChatContentReplacer
from blossom.pipeline.simple_pipeline import SimplePipeline
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.SYSTEM, content="openai developed you"),
            ChatMessage(role=ChatRole.USER, content="who developed you"),
            ChatMessage(role=ChatRole.ASSISTANT, content="OpenAI developed me"),
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
