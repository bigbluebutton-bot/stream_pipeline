# module_classes.py
import copy
from abc import ABC, abstractmethod
import threading
from typing import  Any, Dict, List, Optional, Tuple, Union, final, NamedTuple
import time
import uuid
import grpc # type: ignore
from prometheus_client import Gauge, Summary, Counter

from . import data_pb2
from . import data_pb2_grpc
from .error import Error, exception_to_error
from .data_package import DataPackage, DataPackageModule, DataPackagePhase, DataPackagePhaseController

# Metrics to track time spent on processing modules

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
        self._name = name if name else "M-" + self.__class__.__name__
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

    def run(self, dp: DataPackage, dpc: DataPackagePhaseController, dpp: DataPackagePhase, parent_module: Optional[DataPackageModule] = None) -> None:
        """
        Wrapper method that executes the module's main logic within a thread-safe context.
        Measures and records the execution time and waiting time.
        """
        dpm = DataPackageModule()
        dpm.module_id=self._id
        dpm.module_name=self._name
        dpm.running=True
        dpm.start_time=0.0
        dpm.end_time=0.0
        dpm.waiting_time=0.0
        dpm.total_time=0.0
        dpm.success=True
        dpm.error=None
        
        # Add the module to the parent module if it exists
        if parent_module:
            parent_module.sub_modules.append(dpm)
        else:
            dpp.modules.append(dpm)
        
        start_time = time.time()
        dpm.start_time = start_time
        waiting_time = 0.0
        if self._use_mutex:
            self._mutex.acquire()
            waiting_time = time.time() - start_time
            dpm.waiting_time = waiting_time
        
        
        # Create a thread to execute the execute method
        execute_thread = threading.Thread(target=self._execute_with_result, args=(dp, dpc, dpp, dpm))
        execute_thread.start_context = threading.current_thread().name # type: ignore
        execute_thread.timed_out = False # type: ignore
        execute_thread.start()
        execute_thread.join(self._timeout)
        execute_thread.timed_out = True # type: ignore

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
            dp.success = False
            if dpm.error:
                dp.errors.append(dpm.error)
        
        end_time = time.time()
        dpm.end_time = end_time
        total_time = end_time - start_time
        dpm.total_time = total_time
        dpm.running = False
        
        if self._use_mutex:
            self._mutex.release()

        
        


    def _execute_with_result(self, data: DataPackage, dpc: DataPackagePhaseController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
        """
        Helper method to execute the `execute` method and store the result in a container.
        """
        try:
            self.execute(data, dpc, dpp, dpm)
        except Exception as e:
            current_thread = threading.current_thread()
            if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                # print(f"WARNING: Execution of module {self._name} was interrupted due to timeout.")
                return
            dpm.success = False
            dpm.error = exception_to_error(e)

    @abstractmethod
    def execute(self, data: DataPackage, data_package_controller: DataPackagePhaseController, data_package_phase: DataPackagePhase, data_package_module: DataPackageModule) -> None:
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
    def execute(self, data: DataPackage, data_package_controller: DataPackagePhaseController, data_package_phase: DataPackagePhase, data_package_module: DataPackageModule) -> None:
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
    def execute(self, data: DataPackage, dpc: DataPackagePhaseController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
        """
        Executes the true_module if condition is met, otherwise executes the false_module.
        """
        if self.condition(data):
            self.true_module.run(data, dpc, dpp, dpm)
        else:
            self.false_module.run(data, dpc, dpp, dpm)

class CombinationModule(Module):
    """
    Class for modules that combine multiple modules sequentially.
    """
    @final
    def __init__(self, modules: List[Module], options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.modules = modules
    
    @final
    def execute(self, data: DataPackage, dpc: DataPackagePhaseController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
        """
        Executes each module in the list sequentially, passing the output of one as the input to the next.
        """
        for i, module in enumerate(self.modules):
            module.run(data, dpc, dpp, dpm)
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
    def execute(self, data: DataPackage, dpc: DataPackagePhaseController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
        address = f"{self.host}:{self.port}"
        with grpc.insecure_channel(address) as channel:
            stub = data_pb2_grpc.ModuleServiceStub(channel)
            dp_grpc = data.to_grpc()
            dpc_id = dpc.id
            dpp_id = dpp.id
            dpm_id = dpm.id
            data_grpc = data_pb2.RequestDP(data_package=dp_grpc, data_package_controller_id=dpc_id, data_package_phase_id=dpp_id, data_package_module_id=dpm_id)
            response = stub.run(data_grpc)

            if response.error and response.error.ListFields():
                error = Error()
                error.set_from_grpc(response.error)
                raise error.to_exception()
            else:
                data.set_from_grpc(response.data_package)