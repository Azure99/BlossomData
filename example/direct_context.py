from blossom.context import Context
from blossom.schema import user, system

context = Context()

response = context.chat_completion(
    model="gpt-4o-mini",
    messages=[
        system("You are a cute cat. Reply in Chinese."),
        user("Hello"),
    ],
    extra_params={
        "temperature": 0.3,
    },
)

print(response)
