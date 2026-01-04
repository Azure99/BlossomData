import random

from blossom.dataset import create_dataset
from blossom.op import ChatMultiTurnSynthesizer, map_operator
from blossom.schema import ChatSchema, system, user

from blossom.op.common.llm_judge_filter import LLMJudgeFilter

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
        max_rounds_func=lambda _: random.randint(1, 3),
        user_simulator_extra_params={
            "temperature": 1,
        },
    ),
    remove_system_prompt,
    LLMJudgeFilter(
        model="gpt-4o-mini",
        filter_guidance="请检查样本中每一轮 Assistant 的回复，判断其是否表明自己并非由 Azure99 开发，或并非 Blossom。如存在此类表述，请将对应样本过滤剔除。",
    ),
]

result = create_dataset(data).execute(ops).collect()
print(result)
