

from dataclasses import dataclass
from typing import Any


@dataclass
class DataPackage:
    """
    Class which contains the data and metadata for a pipeline process and will be passed through the pipeline and between modules.
    Attributes:
        id (str): Unique identifier for the data package.
        pipeline_executer_id (str): ID of the pipeline executor handling this package.
        sequence_number (int): The sequence number of the data package.
        data (Any): The actual data contained in the package.
    """
    id: str
    pipeline_executer_id: str
    sequence_number: int
    data: Any = None
    success: bool = False
    message: str = ""