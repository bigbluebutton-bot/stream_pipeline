from dataclasses import dataclass, field
from typing import Any, List, Union
import uuid

from .thread_safe_class import ThreadSafeClass
from .error import Error



@dataclass
class DataPackageModule (ThreadSafeClass):
    """
    Class which contains metadata for a module that has processed a data package.
    Attributes:
        module_id (str):            ID of the module.
        running (bool):             Indicates if the module is running.
        start_time (float):         Time when the module was started.
        end_time (float):           Time when the module finished.
        waiting_time (float):       Time spent waiting for the mutex to unlock.
        processing_time (float):    Time spent processing the data package.
        total_time (float):         Total time spent processing the data package.
        success (bool):             Indicates if the process was successful.
        error (Exception or Error): Contains the error. Can be set as type Exception or Error and will be converted to Error.
    """
    module_id: str = field(default_factory=lambda: "Module-" + str(uuid.uuid4()))
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    waiting_time: float = 0.0
    processing_time: float = 0.0
    total_time: float = 0.0
    success: bool = True
    error: Union[Exception, Error, None] = None



@dataclass
class DataPackage (ThreadSafeClass):
    """
    Class which contains the data and metadata for a pipeline process and will be passed through the pipeline and between modules.
    Attributes:
        id (str) immutable:                     Unique identifier for the data package.
        pipeline_id (str):                      ID of the pipeline handling this package.
        pipeline_executer_id (str) immutable:   ID of the pipeline executor handling this package.
        sequence_number (int):                  The sequence number of the data package.
        modules (List[DataPackageModule]):      List of modules that have processed the data package. Including measurements.
        data (Any):                             The actual data contained in the package.
        success (bool):                         Indicates if the process was successful. If not successful, the error attribute should be set.
        message (str):                          Info message.
        errors (Error):                         List of errors that occurred during the processing of the data package.
    """
    id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    pipeline_id: str = ""
    pipeline_executer_id: str = ""
    sequence_number: int = -1
    modules: List[DataPackageModule] = field(default_factory=list)  # Replace Any with DataPackageModule if defined
    data: Any = None
    running: bool = False
    success: bool = True
    message: str = ""
    errors: List[Union[Error, Exception, None]] = field(default_factory=list)

    # Immutable attributes
    _immutable_attributes: List[str] = field(default_factory=lambda: 
                                            [
                                                'id',
                                                'pipeline_id',
                                                'pipeline_executer_id',
                                            ]
                                        )