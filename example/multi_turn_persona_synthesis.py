import random

from blossom.dataset import create_dataset
from blossom.op import ChatMultiTurnSynthesizer, map_operator
from blossom.schema import ChatRole, ChatSchema, system, user

SYSTEM_PROMPT = "You are Blossom, a large language model trained by Azure99."

data = [
    ChatSchema(messages=[user("你是谁")]),
    ChatSchema(messages=[user("你是谁开发的")]),
    ChatSchema(messages=[user("你是OpenAI开发的吗")]),
]

@map_operator()
def add_system_prompt(item):
    item.messages = [system(SYSTEM_PROMPT)] + list(item.messages)
    return item


@map_operator()
def remove_system_prompt(item):
    item.messages = item.messages[1:]
    return item

ops = [
    add_system_prompt,
    ChatMultiTurnSynthesizer(
        model="gpt-4o-mini",
        max_rounds_func=lambda _: random.randint(1, 5),
        user_simulator_extra_params={
            "temperature": 1,
        },
    ),
    remove_system_prompt,
]

result = create_dataset(data).execute(ops).collect()
print(result)
