from blossom.dataset import create_dataset

from blossom.op import ChatDistill
from blossom.schema import ChatSchema, user, text_content, image_content

data = [
    ChatSchema(
        messages=[
            user(
                [
                    text_content("Please generate a detailed caption for the image."),
                    image_content(
                        "https://www.rainng.com/wp-content/uploads/2024/04/logo-blossom.jpg"
                    ),
                    # image_content_from_file("logo-blossom.jpg", target_size=128),
                    # image_content_from_image(Image.open("logo-blossom.jpg")),
                ]
            ),
        ]
    )
]

ops = [
    ChatDistill(model="gpt-4o-mini"),
]

result = create_dataset(data).execute(ops).collect()
print(result)
