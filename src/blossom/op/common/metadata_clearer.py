from blossom.op.map_operator import MapOperator
from blossom.schema.schema import Schema


class MetadataClearer(MapOperator):
    """
    Clear all metadata on each item.
    """

    def process_item(self, item: Schema) -> Schema:
        item.metadata = {}
        return item
