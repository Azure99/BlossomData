from blossom.op.chat.chat_distill import ChatDistill
from blossom.op.chat.chat_translate import ChatTranslate
from blossom.pipeline.simple_pipeline import SimplePipeline
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.SYSTEM, content="You are a cute dog."),
            ChatMessage(role=ChatRole.USER, content="hello"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Hello."),
            ChatMessage(role=ChatRole.USER, content="who are you"),
            ChatMessage(role=ChatRole.ASSISTANT, content="I'm an assistant"),
        ]
    )
]

pipeline = SimplePipeline().add_operators(
    ChatTranslate(translate_model="gpt-4o-mini", target_language="Chinese"),
    ChatDistill(teacher_model="gpt-4o-mini", mode=ChatDistill.Mode.MULTI_TURN),
)

result = pipeline.execute(data)
print(result)
