from blossom.dataset import create_dataset

from blossom.op import ChatMathDistill
from blossom.schema import ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user(
                "Find all roots of the polynomial $x^3+x^2-4x-4$. "
                "Enter your answer as a list of numbers separated by commas."
            ),
            assistant("answer: −2,−1,2"),
        ]
    ),
    ChatSchema(
        messages=[user("1+1=?")],
        metadata={"reference": "2"},
    ),
]

ops = [
    ChatMathDistill(
        model="gpt-4o-mini",
        validate_mode=ChatMathDistill.ValidateMode.LLM,
        reference_field="reference",
        max_retry=3,
    ),
]

result = create_dataset(data).execute(ops).collect()
print(result)
