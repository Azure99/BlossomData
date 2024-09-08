from blossom.op.chat.chat_math_distill import ChatMathDistill
from blossom.pipeline.simple_pipeline import SimplePipeline
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.USER,
                content="Find all roots of the polynomial $x^3+x^2-4x-4$. Enter your answer as a list of numbers separated by commas.",
            ),
            ChatMessage(role=ChatRole.ASSISTANT, content="−2,−1,2"),
        ]
    )
]

pipeline = SimplePipeline().add_operators(
    ChatMathDistill(
        teacher_model="gpt-4o-mini",
        validate_mode=ChatMathDistill.ValidateMode.LLM,
        max_retry=3,
    ),
)

result = pipeline.execute(data)
print(result)
