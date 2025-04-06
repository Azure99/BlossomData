import random
from blossom.dataset import create_dataset

from blossom.dataset import DatasetEngine, load_dataset
from blossom.op.chat.chat_distiller import ChatDistiller
from blossom.op.chat.chat_translator import ChatTranslator
from blossom.schema import ChatSchema, user, assistant
from blossom.schema.text_schema import TextSchema

example_data = [
    ChatSchema(
        id=str(i),
        messages=[
            user(f"What is 1 + {i} equal to"),
            assistant("I can't answer that question."),
        ],
        metadata={"country": random.choice(["US", "CN"])},
    )
    for i in range(32)
]


create_dataset(example_data).write_json("example_data.jsonl")
(
    load_dataset("example_data.jsonl", engine=DatasetEngine.RAY)
    .filter(lambda x: x.metadata["country"] == "US")
    .shuffle()
    .limit(8)
    .repartition(2)
    .execute(
        [
            ChatTranslator(model="gpt-4o-mini", target_language="Chinese"),
            ChatDistiller(model="gpt-4o-mini", parallel=4),
        ]
    )
    .map(
        lambda x: TextSchema(
            id=x.id,
            content=f"{x.messages[0].content}\n{x.messages[1].content}",
            metadata=x.metadata,
        )
    )
    .write_json("processed_example_data")
)
