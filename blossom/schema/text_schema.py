from blossom.schema.schema import Schema, SchemaType


class TextSchema(Schema):
    type: str = SchemaType.TEXT
    content: str
