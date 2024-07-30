from dataclasses import dataclass, field
import pickle
from typing import Any, List, Union
import uuid

from . import data_pb2

from .thread_safe_class import ThreadSafeClass
from .error import Error, exception_to_error



@dataclass
class DataPackageModule(ThreadSafeClass):
    """
    Class which contains metadata for a module that has processed a data package.
    Attributes:
        module_id (str):                        ID of the module.
        running (bool):                         Indicates if the module is running.
        start_time (float):                     Time when the module was started.
        end_time (float):                       Time when the module finished.
        waiting_time (float):                   Time spent waiting for the mutex to unlock.
        processing_time (float):                Time spent processing the data package.
        total_time (float):                     Total time spent processing the data package.
        sub_modules (List[DataPackageModule]):  List of sub-modules that have processed the data package. Including measurements.
        message (str):                          Info message.
        success (bool):                         Indicates if the process was successful.
        error (Exception or Error):             Contains the error. Can be set as type Exception or Error and will be converted to Error.
    """
    module_id: str = field(default_factory=lambda: "Module-" + str(uuid.uuid4()))
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    waiting_time: float = 0.0
    processing_time: float = 0.0
    total_time: float = 0.0
    sub_modules: List['DataPackageModule'] = field(default_factory=list)
    message: str = ""
    success: bool = True
    error: Union[Exception, Error, None] = None

    def set_from_grpc(self, grpc_module):
        self.module_id = grpc_module.module_id
        self.running = grpc_module.running
        self.start_time = grpc_module.start_time
        self.end_time = grpc_module.end_time
        self.waiting_time = grpc_module.waiting_time
        self.processing_time = grpc_module.processing_time
        self.total_time = grpc_module.total_time

        existing_sub_modules = {sub_module.module_id: sub_module for sub_module in self.sub_modules}
        for module in grpc_module.sub_modules:
            if module:
                if module.module_id in existing_sub_modules:
                    existing_sub_modules[module.module_id].set_from_grpc(module)
                else:
                    new_sub_module = DataPackageModule()
                    new_sub_module.set_from_grpc(module)
                    self.sub_modules.append(new_sub_module)

        self.message = grpc_module.message
        self.success = grpc_module.success
        if grpc_module.error and grpc_module.error.ListFields():
            if self.error is None:
                self.error = Error()
            self.error.set_from_grpc(grpc_module.error)
        else:
            self.error = None

    def to_grpc(self):
        grpc_module = data_pb2.DataPackageModule()
        grpc_module.module_id = self.module_id
        grpc_module.running = self.running
        grpc_module.start_time = self.start_time
        grpc_module.end_time = self.end_time
        grpc_module.waiting_time = self.waiting_time
        grpc_module.processing_time = self.processing_time
        grpc_module.total_time = self.total_time
        grpc_module.sub_modules.extend([module.to_grpc() for module in self.sub_modules])
        grpc_module.message = self.message
        grpc_module.success = self.success
        if isinstance(self.error, Exception):
            self.error = exception_to_error(self.error)
        if self.error:
            grpc_module.error.CopyFrom(self.error.to_grpc())
        else:
            grpc_module.error.Clear()
        return grpc_module

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
        errors (Error):                         List of errors that occurred during the processing of the data package.
    """
    id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    pipeline_id: str = ""
    pipeline_executer_id: str = ""
    sequence_number: int = -1
    modules: List[DataPackageModule] = field(default_factory=list)
    data: Any = None
    running: bool = False
    success: bool = True
    errors: List[Union[Error, Exception, None]] = field(default_factory=list)

    # Immutable attributes
    _immutable_attributes: List[str] = field(default_factory=lambda: 
                                                [
                                                    'id',
                                                    'pipeline_id',
                                                    'pipeline_executer_id',
                                                ]
                                            )
    
    def set_from_grpc(self, grpc_package):
        temp_immutable_attributes = self._immutable_attributes
        self._immutable_attributes = []

        self.pipeline_id = grpc_package.pipeline_id
        self.pipeline_executer_id = grpc_package.pipeline_executer_id
        self.sequence_number = grpc_package.sequence_number

        existing_modules = {module.module_id: module for module in self.modules}
        for module in grpc_package.modules:
            if module:
                if module.module_id in existing_modules:
                    existing_modules[module.module_id].set_from_grpc(module)
                else:
                    new_module = DataPackageModule()
                    new_module.set_from_grpc(module)
                    self.modules.append(new_module)

        self.data = pickle.loads(grpc_package.data)
        self.running = grpc_package.running
        self.success = grpc_package.success

        existing_errors = {error.id: error for error in self.errors}
        for error in grpc_package.errors:
            if error:
                if error.id in existing_errors:
                    existing_errors[error.id].set_from_grpc(error)
                else:
                    new_error = Error()
                    new_error.set_from_grpc(error)
                    self.errors.append(new_error)

        self._immutable_attributes = temp_immutable_attributes

    def to_grpc(self):
        grpc_package = data_pb2.DataPackage()
        grpc_package.pipeline_id = self.pipeline_id
        grpc_package.pipeline_executer_id = self.pipeline_executer_id
        grpc_package.sequence_number = self.sequence_number
        grpc_package.modules.extend([module.to_grpc() for module in self.modules])
        grpc_package.data = pickle.dumps(self.data)
        grpc_package.running = self.running
        grpc_package.success = self.success
        grpc_package.errors.extend([error.to_grpc() for error in self.errors])
        return grpc_package