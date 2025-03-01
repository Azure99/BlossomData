from blossom.schema.base_schema import BaseSchema, SchemaType


class TextSchema(BaseSchema):
    type: str = SchemaType.TEXT
    content: str
