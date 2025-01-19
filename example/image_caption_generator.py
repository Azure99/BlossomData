from blossom.op import ChatDistill, ChatTranslate
from blossom.pipeline import SimplePipeline
from blossom.schema import ChatSchema, user, text_content, image_content


data = [
    ChatSchema(
        messages=[
            user([
                text_content("Please generate a detailed caption for the image."),
                image_content("https://www.rainng.com/wp-content/uploads/2024/04/logo-blossom.jpg"),
            ]),
        ]
    )
]

pipeline = SimplePipeline().add_operators(
    ChatDistill(model="gpt-4o-mini"),
)

result = pipeline.execute(data)
print(result)
