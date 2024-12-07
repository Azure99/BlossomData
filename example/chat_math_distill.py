from blossom.op import ChatMathDistill
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.USER,
                content="Find all roots of the polynomial $x^3+x^2-4x-4$. Enter your answer as a list of numbers separated by commas.",
            ),
            ChatMessage(role=ChatRole.ASSISTANT, content="answer: −2,−1,2"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.USER,
                content="1+1=?",
            ),
        ],
        metadata={"reference": "2"},
    ),
]

pipeline = SimplePipeline().add_operators(
    ChatMathDistill(
        model="gpt-4o-mini",
        validate_mode=ChatMathDistill.ValidateMode.LLM,
        reference_field="reference",
        max_retry=3,
    ),
)

result = pipeline.execute(data)
print(result)
