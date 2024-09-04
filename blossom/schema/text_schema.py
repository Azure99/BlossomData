from blossom.schema.base_schema import BaseSchema, SchemaType


class TextSchema(BaseSchema):
    type: SchemaType = SchemaType.TEXT
    content: str
