from blossom.schema.schema import Schema

SCHEMA_TYPE_TEXT = "text"


class TextSchema(Schema):
    """
    Schema for text data, representing simple text content.

    This schema is used for processing plain text data.

    Attributes:
        type: Always "text"
        content: The text content
    """

    type: str = SCHEMA_TYPE_TEXT
    content: str
