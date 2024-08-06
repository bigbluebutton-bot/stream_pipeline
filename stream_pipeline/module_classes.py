# module_classes.py
import copy
from abc import ABC, abstractmethod
import threading
from typing import  Any, Dict, List, Tuple, Union, final, NamedTuple
import time
import uuid
import grpc # type: ignore
from prometheus_client import Gauge, Summary

from . import data_pb2
from . import data_pb2_grpc
from .error import Error, exception_to_error
from .data_package import DataPackage, DataPackageModule, DataPackagePhase

# Metrics to track time spent on processing modules
MODULE_PROCESSING_TIME = Summary('module_processing_seconds', 'Time spent processing module', ['module_name'])
MODULE_PROCESSING_TIME_WITHOUT_ERROR = Summary('module_processing_seconds_without_error', 'Time spent processing module without error', ['module_name'])
MODULE_PROCESSING_COUNTER = Gauge('module_processing_counter', 'Number of processes executing the module at the moment', ['module_name'])
MODULE_WAITING_TIME = Summary('module_waiting_seconds', 'Time spent waiting before executing the task (mutex)', ['module_name'])
MODULE_TOTAL_TIME = Summary('module_total_seconds', 'Total time spent processing module', ['module_name'])
MODULE_TOTAL_TIME_WITHOUT_ERROR = Summary('module_total_seconds_without_error', 'Total time spent processing module without error', ['module_name'])
MODULE_WAITING_COUNTER = Gauge('module_waiting_counter', 'Number of processes waiting to execute the task (mutex)', ['module_name'])
MODULE_ERROR_COUNTER = Gauge('module_error_counter', 'Number of errors encountered by the module', ['module_name'])

class ModuleOptions(NamedTuple):
    """
    Named tuple to store options for modules.
    Attributes:
        use_mutex (bool): whether to use a mutex lock for thread safety (Default: True)
        timeout (float): timeout to stop executing after x seconds. If 0.0, waits indefinitely (Default: 0.0)
    """
    use_mutex: bool = False
    timeout: float = 0.0

class Module(ABC):
    """
    Abstract base class for modules.
    """
    _locks: Dict[int, threading.RLock] = {}

    def __init__(self, options: ModuleOptions = ModuleOptions(), name: str = ""):
        self._id = "M-" + self.__class__.__name__ + "-" + str(uuid.uuid4()) 
        self._name = name if name else self._id
        self._use_mutex = options.use_mutex
        self._timeout = options.timeout if options.timeout > 0.0 else None

    def get_id(self) -> str:
        return self._id
    
    def get_name(self) -> str:
        return self._name

    @property
    def _mutex(self) -> threading.RLock:
        """
        Provides a reentrant lock for each module instance.
        """
        if id(self) not in self._locks:
            self._locks[id(self)] = threading.RLock()
        return self._locks[id(self)]

    def run(self, data_package: DataPackage, parent_module: Union[DataPackageModule, None] = None, phase: Union[DataPackagePhase, None] = None) -> None:
        """
        Wrapper method that executes the module's main logic within a thread-safe context.
        Measures and records the execution time and waiting time.
        """
        dpm = DataPackageModule()
        dpm.id=self._id
        dpm.running=True
        dpm.start_time=0.0
        dpm.end_time=0.0
        dpm.waiting_time=0.0
        dpm.processing_time=0.0
        dpm.total_time=0.0
        dpm.success=True
        dpm.error=None
        
        # Add the module to the parent module if it exists
        if parent_module:
            parent_module.sub_modules.append(dpm)
        else:
            if phase:
                phase.modules.append(dpm)
            else:
                raise ValueError("Parent module or phase must be provided.")
        
        start_total_time = time.time()
        waiting_time = 0.0
        if self._use_mutex:
            MODULE_WAITING_COUNTER.labels(module_name=self.__class__.__name__).inc()
            self._mutex.acquire()
            waiting_time = time.time() - start_total_time
            dpm.waiting_time = waiting_time
            MODULE_WAITING_TIME.labels(module_name=self.__class__.__name__).observe(waiting_time)
            MODULE_WAITING_COUNTER.labels(module_name=self.__class__.__name__).dec()

        start_time = time.time()
        dpm.start_time = start_total_time
        
        # Create a thread to execute the execute method
        execute_thread = threading.Thread(target=self._execute_with_result, args=(data_package, dpm))
        execute_thread.start_context = threading.current_thread().name # type: ignore
        execute_thread.timed_out = False # type: ignore
        MODULE_PROCESSING_COUNTER.labels(module_name=self.__class__.__name__).inc()
        execute_thread.start()
        execute_thread.join(self._timeout)
        execute_thread.timed_out = True # type: ignore
        MODULE_PROCESSING_COUNTER.labels(module_name=self.__class__.__name__).dec()

        thread_alive = execute_thread.is_alive()
        if thread_alive:
            if thread_alive:
                try:
                    # Raise a TimeoutError to stop the thread
                    raise TimeoutError(f"Execution of module {self._name} timed out after {self._timeout} seconds.")
                except TimeoutError as te:
                    dpm.success = False
                    dpm.error = exception_to_error(te)

        if not dpm.success:
            data_package.success = False
            data_package.errors.append(dpm.error)
            MODULE_ERROR_COUNTER.labels(module_name=self.__class__.__name__).inc()
        
        end_time = time.time()
        dpm.end_time = end_time
        processing_time = end_time - start_time
        dpm.processing_time = processing_time
        total_time = end_time - start_total_time
        dpm.total_time = total_time
        dpm.running = False


        if data_package.success:
            MODULE_PROCESSING_TIME_WITHOUT_ERROR.labels(module_name=self.__class__.__name__).observe(processing_time)
            MODULE_TOTAL_TIME_WITHOUT_ERROR.labels(module_name=self.__class__.__name__).observe(total_time)
        
        MODULE_PROCESSING_TIME.labels(module_name=self.__class__.__name__).observe(processing_time)
        MODULE_TOTAL_TIME.labels(module_name=self.__class__.__name__).observe(total_time)
        
        if self._use_mutex:
            self._mutex.release()

        
        


    def _execute_with_result(self, data: DataPackage, dpm: DataPackageModule) -> None:
        """
        Helper method to execute the `execute` method and store the result in a container.
        """
        try:
            self.execute(data, dpm)
        except Exception as e:
            current_thread = threading.current_thread()
            if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                # print(f"WARNING: Execution of module {self._name} was interrupted due to timeout.")
                return
            dpm.success = False
            dpm.error = exception_to_error(e)

    @abstractmethod
    def execute(self, data: DataPackage, data_package_module: DataPackageModule) -> None:
        """
        Abstract method to be implemented by subclasses.
        Performs an operation on the data package.
        """
        pass

    def __deepcopy__(self, memo: Dict[int, Any]) -> Any:
        # Check if the object is already in memo
        if id(self) in memo:
            return memo[id(self)]
        
        # Create a copy of the object
        copy_obj = self.__class__.__new__(self.__class__)
        
        # Store the copy in memo
        memo[id(self)] = copy_obj
        
        # Deep copy all instance attributes
        for k, v in self.__dict__.items():
            setattr(copy_obj, k, copy.deepcopy(v, memo))
        
        # Ensure that the _locks attribute is copied correctly
        if id(self) in self._locks:
            copy_obj._locks[id(copy_obj)] = threading.RLock()
        
        return copy_obj

class ExecutionModule(Module, ABC):
    def __init__(self, options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)

    """
    Abstract class for modules that perform specific execution tasks.
    """
    @abstractmethod
    def execute(self, data: DataPackage, data_package_module: DataPackageModule) -> None:
        """
        Method to be implemented by subclasses for specific execution logic.
        """
        pass

class ConditionModule(Module, ABC):
    """
    Abstract class for modules that decide between two modules based on a condition.
    """
    @final
    def __init__(self, true_module: Module, false_module: Module, options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.true_module = true_module
        self.false_module = false_module

    @abstractmethod
    def condition(self, data: DataPackage) -> bool:
        """
        Abstract method to be implemented by subclasses to evaluate conditions based on data input.
        """
        return True

    @final
    def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
        """
        Executes the true_module if condition is met, otherwise executes the false_module.
        """
        if self.condition(data):
            self.true_module.run(data, dpm)
        else:
            self.false_module.run(data, dpm)

class CombinationModule(Module):
    """
    Class for modules that combine multiple modules sequentially.
    """
    @final
    def __init__(self, modules: List[Module], options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.modules = modules
    
    @final
    def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
        """
        Executes each module in the list sequentially, passing the output of one as the input to the next.
        """
        for i, module in enumerate(self.modules):
            module.run(data, dpm)
            if not data.success:
                break


class ExternalModule(Module):
    """
    Class for a module that runs on a different server. Using gRPC for communication.
    """
    def __init__(self, host: str, port: int, options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.host: str = host
        self.port: int = port

    @final
    def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
        address = f"{self.host}:{self.port}"
        with grpc.insecure_channel(address) as channel:
            stub = data_pb2_grpc.ModuleServiceStub(channel)
            dp_grpc = data.to_grpc()
            dpm_grpc = dpm.to_grpc()
            data_grpc = data_pb2.RequestDPandDPM(data_package=dp_grpc, data_package_module=dpm_grpc) # type: ignore
            response = stub.run(data_grpc)

            # Debugging output
            # print(f"Type of response.error: {type(response.error)}")
            # print(f"repr of response.error: {repr(response.error)}")
            # print(f"Fields of response.error: {response.error.ListFields()}")

            if response.error and response.error.ListFields():
                error = Error()
                error.set_from_grpc(response.error)
                raise error.to_exception()
            else:
                data.set_from_grpc(response.data_package)