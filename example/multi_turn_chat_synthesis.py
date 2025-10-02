from blossom.dataset import create_dataset

from blossom.op import ChatMultiTurnSynthesizer
from blossom.schema import ChatSchema, user, assistant

data = [
    ChatSchema(
        messages=[
            user("你好"),
            assistant("你好，有什么我可以帮助你的吗？"),
        ]
    ),
]

ops = [
    ChatMultiTurnSynthesizer(
        model="gpt-4o-mini",
        max_rounds=3,
        user_simulator_extra_params={
            "temperature": 1,
        },
    ),
]

result = create_dataset(data).execute(ops).collect()
print(result)
