from blossom.schema.schema import Schema

SCHEMA_TYPE_TEXT = "text"


class TextSchema(Schema):
    type: str = SCHEMA_TYPE_TEXT
    content: str
