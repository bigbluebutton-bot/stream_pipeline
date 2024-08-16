from dataclasses import dataclass, field
import pickle
from typing import Generic, List, Optional, TypeVar, Union
import uuid

from . import data_pb2
from .thread_safe_class import ThreadSafeClass
from .error import Error, exception_to_error



@dataclass
class DataPackageModule(ThreadSafeClass):
    """
    Represents metadata for a module that has processed a data package.
    
    Attributes:
        id (str):                               ID of the DP module.
        module_id (str):                        ID of the module.
        module_name (str):                      Name of the module.
        running (bool):                         Indicates if the module is currently running.
        start_time (float):                     Timestamp when the module started.
        end_time (float):                       Timestamp when the module finished.
        waiting_time (float):                   Time spent waiting for the mutex to unlock.
        total_time (float):                     Total time spent on the data package processing.
        sub_modules (List[DataPackageModule]):  List of sub-modules that processed the data package.
        message (str):                          Informational message.
        success (bool):                         Indicates if the processing was successful.
        error (Union[Exception, Error, None]):  Error encountered during processing, if any.
    """
    _id: str = field(default_factory=lambda: "DP-M-" + str(uuid.uuid4()))
    _module_id: str = ""
    _module_name: str = ""
    _running: bool = False
    _start_time: float = 0.0
    _end_time: float = 0.0
    _waiting_time: float = 0.0
    _total_time: float = 0.0
    _sub_modules: List['DataPackageModule'] = field(default_factory=list)
    _message: str = ""
    _success: bool = True
    _error: Optional[Error] = None
    
    _ThreadSafeClass__immutable_attributes: List[str] = field(default_factory=lambda: [])

    @property
    def id(self) -> str:
        return self._get_attribute('id')
    
    @id.setter
    def id(self, value: str) -> None:
        self._set_attribute('id', value)
    
    @property
    def module_id(self) -> str:
        return self._get_attribute('module_id')
    
    @module_id.setter
    def module_id(self, value: str) -> None:
        self._set_attribute('module_id', value)
    
    @property
    def module_name(self) -> str:
        return self._get_attribute('module_name')
    
    @module_name.setter
    def module_name(self, value: str) -> None:
        self._set_attribute('module_name', value)
    
    @property
    def running(self) -> bool:
        return self._get_attribute('running')
    
    @running.setter
    def running(self, value: bool) -> None:
        self._set_attribute('running', value)
    
    @property
    def start_time(self) -> float:
        return self._get_attribute('start_time')
    
    @start_time.setter
    def start_time(self, value: float) -> None:
        self._set_attribute('start_time', value)
    
    @property
    def end_time(self) -> float:
        return self._get_attribute('end_time')
    
    @end_time.setter
    def end_time(self, value: float) -> None:
        self._set_attribute('end_time', value)
    
    @property
    def waiting_time(self) -> float:
        return self._get_attribute('waiting_time')
    
    @waiting_time.setter
    def waiting_time(self, value: float) -> None:
        self._set_attribute('waiting_time', value)
    
    @property
    def total_time(self) -> float:
        return self._get_attribute('total_time')
    
    @total_time.setter
    def total_time(self, value: float) -> None:
        self._set_attribute('total_time', value)
    
    @property
    def sub_modules(self) -> List['DataPackageModule']:
        return self._get_attribute('sub_modules')
    
    @sub_modules.setter
    def sub_modules(self, value: List['DataPackageModule']) -> None:
        self._set_attribute('sub_modules', value)
    
    @property
    def message(self) -> str:
        return self._get_attribute('message')
    
    @message.setter
    def message(self, value: str) -> None:
        self._set_attribute('message', value)
    
    @property
    def success(self) -> bool:
        return self._get_attribute('success')
    
    @success.setter
    def success(self, value: bool) -> None:
        self._set_attribute('success', value)
    
    @property
    def error(self) -> Optional[Error]:
        return self._get_attribute('error')
    
    @error.setter
    def error(self, value: Union[Exception, Error, None]) -> None:
        if isinstance(value, Exception):
            value = exception_to_error(value)
        self._set_attribute('error', value)

    def set_from_grpc(self, grpc_module: data_pb2.DataPackageModule) -> None:
        """
        Updates the current instance with data from a gRPC module.
        """
        temp_immutable_attributes = self._ThreadSafeClass__immutable_attributes
        self._ThreadSafeClass__immutable_attributes = []

        self.id = grpc_module.id
        self.module_id = grpc_module.module_id
        self.module_name = grpc_module.module_name
        self.running = grpc_module.running
        self.start_time = grpc_module.start_time
        self.end_time = grpc_module.end_time
        self.waiting_time = grpc_module.waiting_time
        self.total_time = grpc_module.total_time

        existing_sub_modules = {sub_module.id: sub_module for sub_module in self.sub_modules}
        for module in grpc_module.sub_modules:
            if module:
                if module.id in existing_sub_modules:
                    existing_sub_modules[module.id].set_from_grpc(module)
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
        
        self._ThreadSafeClass__immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackageModule:
        """
        Converts the current instance to a gRPC module.
        """
        grpc_module = data_pb2.DataPackageModule()
        grpc_module.id = self.id
        grpc_module.module_id = self.module_id
        grpc_module.module_name = self.module_name
        grpc_module.running = self.running
        grpc_module.start_time = self.start_time
        grpc_module.end_time = self.end_time
        grpc_module.waiting_time = self.waiting_time
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
class DataPackagePhase(ThreadSafeClass):
    """
    Represents metadata for a phase that has processed a data package.
    
    Attributes:
        id (str):                           ID of the DP phase.
        phase_id (str):                     ID of the phase.
        phase_name (str):                   Name of the phase.
        running (bool):                     Indicates if the phase is currently running.
        start_time (float):                 Timestamp when the phase started.
        end_time (float):                   Timestamp when the phase finished.
        total_time (float):                 Total time spent on the phase.
        modules (List[DataPackageModule]):  List of modules that processed the data package.
    """
    _id: str = field(default_factory=lambda: "DP-PP-" + str(uuid.uuid4()))
    _phase_id: str = ""
    _phase_name: str = ""
    _running: bool = False
    _start_time: float = 0.0
    _end_time: float = 0.0
    _total_time: float = 0.0
    _modules: List['DataPackageModule'] = field(default_factory=list)

    # Immutable attributes
    _ThreadSafeClass__immutable_attributes: List[str] = field(default_factory=lambda: ['id'])
    
    @property
    def id(self) -> str:
        return self._get_attribute('id')
    
    @id.setter
    def id(self, value: str) -> None:
        self._set_attribute('id', value)
    
    @property
    def phase_id(self) -> str:
        return self._get_attribute('phase_id')
    
    @phase_id.setter
    def phase_id(self, value: str) -> None:
        self._set_attribute('phase_id', value)
    
    @property
    def phase_name(self) -> str:
        return self._get_attribute('phase_name')
    
    @phase_name.setter
    def phase_name(self, value: str) -> None:
        self._set_attribute('phase_name', value)
    
    @property
    def running(self) -> bool:
        return self._get_attribute('running')
    
    @running.setter
    def running(self, value: bool) -> None:
        self._set_attribute('running', value)
    
    @property
    def start_time(self) -> float:
        return self._get_attribute('start_time')
    
    @start_time.setter
    def start_time(self, value: float) -> None:
        self._set_attribute('start_time', value)
    
    @property
    def end_time(self) -> float:
        return self._get_attribute('end_time')
    
    @end_time.setter
    def end_time(self, value: float) -> None:
        self._set_attribute('end_time', value)
    
    @property
    def total_time(self) -> float:
        return self._get_attribute('total_time')
    
    @total_time.setter
    def total_time(self, value: float) -> None:
        self._set_attribute('total_time', value)
    
    @property
    def modules(self) -> List['DataPackageModule']:
        return self._get_attribute('modules')
    
    @modules.setter
    def modules(self, value: List['DataPackageModule']) -> None:
        self._set_attribute('modules', value)

    def set_from_grpc(self, grpc_phase: data_pb2.DataPackagePhase) -> None:
        """
        Updates the current instance with data from a gRPC phase.
        """
        temp_immutable_attributes = self._ThreadSafeClass__immutable_attributes
        self._ThreadSafeClass__immutable_attributes = []

        self.id = grpc_phase.id
        self.phase_id = grpc_phase.phase_id
        self.phase_name = grpc_phase.phase_name
        self.running = grpc_phase.running
        self.start_time = grpc_phase.start_time
        self.end_time = grpc_phase.end_time
        self.total_time = grpc_phase.total_time

        existing_modules = {module.id: module for module in self.modules}
        for module in grpc_phase.modules:
            if module:
                if module.id in existing_modules:
                    existing_modules[module.id].set_from_grpc(module)
                else:
                    new_module = DataPackageModule()
                    new_module.set_from_grpc(module)
                    self.modules.append(new_module)

        self._ThreadSafeClass__immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackagePhase:
        """
        Converts the current instance to a gRPC phase.
        """
        grpc_phase = data_pb2.DataPackagePhase()
        grpc_phase.id = self.id
        grpc_phase.phase_id = self.phase_id
        grpc_phase.phase_name = self.phase_name
        grpc_phase.running = self.running
        grpc_phase.start_time = self.start_time
        grpc_phase.end_time = self.end_time
        grpc_phase.total_time = self.total_time
        grpc_phase.modules.extend([module.to_grpc() for module in self.modules])
        return grpc_phase


@dataclass
class DataPackageController(ThreadSafeClass):
    """
    Represents metadata for a phase execution that has processed a data package.
    
    Attributes:
        id (str):                           ID of the DP controller
        controller_id (str):                ID of the controller.
        controller_name (str):              Name of the controller.
        mode (str):                         Type of phase execution (e.g., NOT_PARALLEL, ORDER_BY_SEQUENCE, FIRST_WINS, NO_ORDER).
        workers (int):                      Number of workers used for executing phases in parallel. 0 means no multi-threading.
        sequence_number (int):              Sequence number of the data package.
        running (bool):                     Indicates if the phase execution is currently running.
        start_time (float):                 Timestamp when the phase execution started.
        end_time (float):                   Timestamp when the phase execution finished.
        _input_waiting_time (float):        Time waiting for teh data package to be processed.
        _output_waiting_time (float):       Time waiting for the data package to be output. Maybe because of sorting. (mode)
        total_time (float):                 Total time spent on phase execution.
        phases (List[DataPackagePhase]):    List of phases that processed the data package.
    """
    _id: str = field(default_factory=lambda: "DP-C-" + str(uuid.uuid4()))
    _controller_id: str = ""
    _controller_name: str = ""
    _mode: str = "NOT_PARALLEL"
    _workers: int = 1
    _sequence_number: int = -1
    _running: bool = False
    _start_time: float = 0.0
    _end_time: float = 0.0
    _input_waiting_time: float = 0.0
    _output_waiting_time: float = 0.0
    _total_time: float = 0.0
    _phases: List[DataPackagePhase] = field(default_factory=list)
    
    _ThreadSafeClass__immutable_attributes: List[str] = field(default_factory=lambda: ['id'])

    @property
    def id(self) -> str:
        return self._get_attribute('id')
    
    @id.setter
    def id(self, value: str) -> None:
        self._set_attribute('id', value)
    
    @property
    def controller_id(self) -> str:
        return self._get_attribute('controller_id')
    
    @controller_id.setter
    def controller_id(self, value: str) -> None:
        self._set_attribute('controller_id', value)
    
    @property
    def controller_name(self) -> str:
        return self._get_attribute('controller_name')
    
    @controller_name.setter
    def controller_name(self, value: str) -> None:
        self._set_attribute('controller_name', value)
    
    @property
    def mode(self) -> str:
        return self._get_attribute('mode')
    
    @mode.setter
    def mode(self, value: str) -> None:
        self._set_attribute('mode', value)
    
    @property
    def workers(self) -> int:
        return self._get_attribute('workers')
    
    @workers.setter
    def workers(self, value: int) -> None:
        self._set_attribute('workers', value)
    
    @property
    def sequence_number(self) -> int:
        return self._get_attribute('sequence_number')
    
    @sequence_number.setter
    def sequence_number(self, value: int) -> None:
        self._set_attribute('sequence_number', value)
    
    @property
    def running(self) -> bool:
        return self._get_attribute('running')
    
    @running.setter
    def running(self, value: bool) -> None:
        self._set_attribute('running', value)
    
    @property
    def start_time(self) -> float:
        return self._get_attribute('start_time')
    
    @start_time.setter
    def start_time(self, value: float) -> None:
        self._set_attribute('start_time', value)
    
    @property
    def end_time(self) -> float:
        return self._get_attribute('end_time')
    
    @end_time.setter
    def end_time(self, value: float) -> None:
        self._set_attribute('end_time', value)
    
    @property
    def input_waiting_time(self) -> float:
        return self._get_attribute('input_waiting_time')
    
    @input_waiting_time.setter
    def input_waiting_time(self, value: float) -> None:
        self._set_attribute('input_waiting_time', value)
        
    @property
    def output_waiting_time(self) -> float:
        return self._get_attribute('output_waiting_time')
    
    @output_waiting_time.setter
    def output_waiting_time(self, value: float) -> None:
        self._set_attribute('output_waiting_time', value)
    
    @property
    def total_time(self) -> float:
        return self._get_attribute('total_time')
    
    @total_time.setter
    def total_time(self, value: float) -> None:
        self._set_attribute('total_time', value)
    
    @property
    def phases(self) -> List[DataPackagePhase]:
        return self._get_attribute('phases')
    
    @phases.setter
    def phases(self, value: List[DataPackagePhase]) -> None:
        self._set_attribute('phases', value)

    def set_from_grpc(self, grpc_execution: data_pb2.DataPackageController) -> None:
        """
        Updates the current instance with data from a gRPC phase execution.
        """
        temp_immutable_attributes = self._ThreadSafeClass__immutable_attributes
        self._ThreadSafeClass__immutable_attributes = []

        self.id = grpc_execution.id
        self.controller_id = grpc_execution.controller_id
        self.controller_name = grpc_execution.controller_name
        self.mode = grpc_execution.mode
        self.workers = grpc_execution.workers
        self.sequence_number = grpc_execution.sequence_number
        self.running = grpc_execution.running
        self.start_time = grpc_execution.start_time
        self.end_time = grpc_execution.end_time
        self.input_waiting_time = grpc_execution.input_waiting_time
        self.output_waiting_time = grpc_execution.output_waiting_time
        self.total_time = grpc_execution.total_time

        existing_phases = {phase.id: phase for phase in self.phases}
        for phase in grpc_execution.phases:
            if phase:
                if phase.id in existing_phases:
                    existing_phases[phase.id].set_from_grpc(phase)
                else:
                    new_phase = DataPackagePhase()
                    new_phase.set_from_grpc(phase)
                    self.phases.append(new_phase)

        self._ThreadSafeClass__immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackageController:
        """
        Converts the current instance to a gRPC phase execution.
        """
        grpc_execution = data_pb2.DataPackageController()
        grpc_execution.id = self.id
        grpc_execution.controller_id = self.controller_id
        grpc_execution.controller_name = self.controller_name
        grpc_execution.mode = self.mode
        grpc_execution.workers = self.workers
        grpc_execution.sequence_number = self.sequence_number
        grpc_execution.running = self.running
        grpc_execution.start_time = self.start_time
        grpc_execution.end_time = self.end_time
        grpc_execution.input_waiting_time = self.input_waiting_time
        grpc_execution.output_waiting_time = self.output_waiting_time
        grpc_execution.total_time = self.total_time
        grpc_execution.phases.extend([phase.to_grpc() for phase in self.phases])
        return grpc_execution

T = TypeVar('T')

@dataclass
class DataPackage(Generic[T], ThreadSafeClass):
    """
    Represents the data and metadata for a pipeline process, passed through the pipeline and between modules.
    
    Attributes:
        id (str):                                       ID of the data package.
        pipeline_id (str):                              ID of the pipeline handling this package.
        pipeline_name (str):                            Name of the pipeline handling this package.
        pipeline_instance_id (str):                     ID of the pipeline instance handling this package.
        controllers (List[DataPackageController]):      List of phases processed in the mode of execution.
        data (Optional[T]):                             Actual data contained in the package.
        running (bool):                                 Indicates if the data package is currently being processed.
        start_time (float):                             Timestamp when the data package started processing.
        end_time (float):                               Timestamp when the data package finished processing.
        total_time (float):                             Total time spent processing the data package.
        success (bool):                                 Indicates if the process was successful.
        errors (List[Optional[Error]]):                 List of errors that occurred during processing.
    """
    _id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    _pipeline_id: str = ""
    _pipeline_name: str = ""
    _pipeline_instance_id: str = ""
    _controllers: List[DataPackageController] = field(default_factory=list)
    _data: Optional[T] = None
    _running: bool = False
    _start_time: float = 0.0
    _end_time: float = 0.0
    _total_time: float = 0.0
    _success: bool = True
    _errors: List[Optional[Error]] = field(default_factory=list)

    # Immutable attributes
    _ThreadSafeClass__immutable_attributes: List[str] = field(default_factory=lambda: ['id'])
    
    @property
    def id(self) -> str:
        return self._get_attribute('id')
    
    @id.setter
    def id(self, value: str) -> None:
        self._set_attribute('id', value)
    
    @property
    def pipeline_id(self) -> str:
        return self._get_attribute('pipeline_id')
    
    @pipeline_id.setter
    def pipeline_id(self, value: str) -> None:
        self._set_attribute('pipeline_id', value)
    
    @property
    def pipeline_name(self) -> str:
        return self._get_attribute('pipeline_name')
    
    @pipeline_name.setter
    def pipeline_name(self, value: str) -> None:
        self._set_attribute('pipeline_name', value)
    
    @property
    def pipeline_instance_id(self) -> str:
        return self._get_attribute('pipeline_instance_id')
    
    @pipeline_instance_id.setter
    def pipeline_instance_id(self, value: str) -> None:
        self._set_attribute('pipeline_instance_id', value)
    
    @property
    def controllers(self) -> List[DataPackageController]:
        return self._get_attribute('controllers')
    
    @controllers.setter
    def controllers(self, value: List[DataPackageController]) -> None:
        self._set_attribute('controllers', value)
    
    @property
    def data(self) -> Optional[T]:
        return self._get_attribute('data')
    
    @data.setter
    def data(self, value: Optional[T]) -> None:
        self._set_attribute('data', value)
    
    @property
    def running(self) -> bool:
        return self._get_attribute('running')
    
    @running.setter
    def running(self, value: bool) -> None:
        self._set_attribute('running', value)
    
    @property
    def start_time(self) -> float:
        return self._get_attribute('start_time')
    
    @start_time.setter
    def start_time(self, value: float) -> None:
        self._set_attribute('start_time', value)
    
    @property
    def end_time(self) -> float:
        return self._get_attribute('end_time')
    
    @end_time.setter
    def end_time(self, value: float) -> None:
        self._set_attribute('end_time', value)
    
    @property
    def total_time(self) -> float:
        return self._get_attribute('total_time')
    
    @total_time.setter
    def total_time(self, value: float) -> None:
        self._set_attribute('total_time', value)
    
    @property
    def success(self) -> bool:
        return self._get_attribute('success')
    
    @success.setter
    def success(self, value: bool) -> None:
        self._set_attribute('success', value)
    
    @property
    def errors(self) -> List[Optional[Error]]:
        return self._get_attribute('errors')
    
    @errors.setter
    def errors(self, value: List[Union[Exception, Error, None]]) -> None:
        new_list = []
        for error in value:
            if isinstance(error, Exception):
                error = exception_to_error(error)
            new_list.append(error)
        self._set_attribute('errors', new_list)

    def set_from_grpc(self, grpc_package: data_pb2.DataPackage) -> None:
        """
        Updates the current instance with data from a gRPC data package.
        """
        temp_immutable_attributes = self._ThreadSafeClass__immutable_attributes
        self._ThreadSafeClass__immutable_attributes = []

        self.id = grpc_package.id
        self.pipeline_id = grpc_package.pipeline_id
        self.pipeline_name = grpc_package.pipeline_name
        self.pipeline_instance_id = grpc_package.pipeline_instance_id

        existing_controllers = {controller.id: controller for controller in self.controllers}
        for controller in grpc_package.controllers:
            if controller:
                if controller.id in existing_controllers:
                    existing_controllers[controller.id].set_from_grpc(controller)
                else:
                    new_controller = DataPackageController()
                    new_controller.set_from_grpc(controller)
                    self.controllers.append(new_controller)

        self.data = pickle.loads(grpc_package.data)
        self.running = grpc_package.running
        self.start_time = grpc_package.start_time
        self.end_time = grpc_package.end_time
        self.total_time = grpc_package.total_time
        self.success = grpc_package.success

        existing_errors = {error.id: error for error in self.errors if error}
        for error in grpc_package.errors:
            if error:
                if error.id in existing_errors:
                    existing_errors[error.id].set_from_grpc(error)
                else:
                    new_error = Error()
                    new_error.set_from_grpc(error)
                    self.errors.append(new_error)

        self._ThreadSafeClass__immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackage:
        """
        Converts the current instance to a gRPC data package.
        """
        grpc_package = data_pb2.DataPackage()
        grpc_package.id = self.id
        grpc_package.pipeline_name = self.pipeline_name
        grpc_package.pipeline_id = self.pipeline_id
        grpc_package.pipeline_instance_id = self.pipeline_instance_id
        grpc_package.controllers.extend([phase.to_grpc() for phase in self.controllers])
        try:
            grpc_package.data = pickle.dumps(self.data)
        except Exception as e:
            if isinstance(e, AttributeError):
                raise RuntimeError(
                            "Failed to serialize data. This likely occurred because the Data class is not defined in its own file. "
                            "When the Data class is defined within the main script, pickle cannot import it properly, leading to a circular import issue. "
                            "To resolve this, define the Data class in a separate file and import it into your main script."
                        ) from e           
            else:
                raise e
        grpc_package.running = self.running
        grpc_package.start_time = self.start_time
        grpc_package.end_time = self.end_time
        grpc_package.total_time = self.total_time
        grpc_package.success = self.success
        grpc_package.errors.extend([error.to_grpc() for error in self.errors if error])
        return grpc_package