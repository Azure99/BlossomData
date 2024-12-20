import copy
from blossom.op import (
    filter_operator,
    map_operator,
    transform_operator,
    context_map_operator,
)
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatRole, ChatSchema, user, assistant


data = [
    ChatSchema(
        messages=[
            user("Just give me a number within 99999"),
            assistant("1"),
        ]
    ),
    ChatSchema(
        messages=[
            user("Hi"),
            assistant("Hi."),
        ]
    ),
]


@transform_operator()
def duplicate_data(items):
    items = items + copy.deepcopy(items)
    for id, item in enumerate(items):
        item.id = str(id)
    return items


@map_operator()
def remove_assistant(item):
    item.messages = [msg for msg in item.messages if msg.role != ChatRole.ASSISTANT]
    return item


@filter_operator()
def filter_hi_prompt(item):
    return item.first_user() != "Hi"


@context_map_operator(parallel=2)
def generate_response(context, item):
    response = context.chat_completion("gpt-4o-mini", [item.first_message()])
    return item.add_assistant(response)


pipeline = SimplePipeline().add_operators(
    duplicate_data, remove_assistant, filter_hi_prompt, generate_response
)

result = pipeline.execute(data)
print(result)
