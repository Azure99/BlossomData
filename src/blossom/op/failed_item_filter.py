from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class FailedItemFilter(Operator):
    """
    Operator that filters out failed items from the dataset.

    This operator removes items that have been marked as failed during previous
    processing steps, or keeps only failed items if reverse is True.
    """

    def __init__(
        self,
        reverse: bool = False,
    ):
        """
        Initialize a FailedItemFilter.

        Args:
            reverse: If True, keep only failed items (default: False)
        """
        super().__init__()
        self.reverse = reverse

    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Process the dataframe by filtering out failed items.

        Args:
            dataframe: Input dataframe

        Returns:
            Filtered dataframe
        """
        return dataframe.filter(lambda item: self.process_item(item) ^ self.reverse)

    @staticmethod
    def process_item(item: Schema) -> bool:
        """
        Check if an item has not failed.

        Args:
            item: Input schema

        Returns:
            True if the item has not failed, False otherwise
        """
        return not item.failed
