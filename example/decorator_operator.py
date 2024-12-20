import copy
from blossom.op import (
    filter_operator,
    map_operator,
    transform_operator,
    context_map_operator,
)
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatMessage, ChatRole, ChatSchema


data = [
    ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.USER, content="Just give me a number within 99999"
            ),
            ChatMessage(role=ChatRole.ASSISTANT, content="1"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="Hi"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Hi."),
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
def filter_hi(item):
    return item.messages[0].content != "Hi"


@context_map_operator(parallel=2)
def generate_response(context, item):
    response = context.single_chat_completion("gpt-4o-mini", item.messages[0].content)
    item.messages.append(ChatMessage(role=ChatRole.ASSISTANT, content=response))
    return item


pipeline = SimplePipeline().add_operators(
    duplicate_data,
    remove_assistant,
    filter_hi,
    generate_response
)

result = pipeline.execute(data)
print(result)
