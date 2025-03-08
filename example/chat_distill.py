from blossom.dataset import create_dataset

from blossom.op import ChatDistill, ChatTranslate
from blossom.schema import ChatSchema, system, user, assistant

data = [
    ChatSchema(
        messages=[
            system("You are a cute dog."),
            user("hello"),
            assistant("Hello."),
            user("who are you"),
            assistant("I'm an assistant"),
        ]
    )
]

ops = [
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
    ChatDistill(model="gpt-4o-mini", strategy=ChatDistill.Strategy.MULTI_TURN),
]

result = create_dataset(data).execute(ops).collect()
print(result)
