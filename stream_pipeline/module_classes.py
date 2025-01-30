# module_classes.py
import copy
from abc import ABC, abstractmethod
import threading
from typing import  Any, Callable, Dict, List, Optional, Tuple, Union, final, NamedTuple
import time
import uuid
import grpc
from prometheus_client import Gauge, Summary, Counter

from . import data_pb2
from . import data_pb2_grpc
from .logger import Error, PipelineLogger, exception_to_error
from .data_package import DataPackage, DataPackageModule, DataPackagePhase, DataPackageController, Status

LockType = type(threading.Lock())   # typically <class '_thread.lock'>
RLockType = type(threading.RLock()) # typically <class '_thread.RLock'>

# Metrics to track time spent on processing modules
MODULE_INPUT_FLOWRATE = Counter("module_input_flowrate", "The flowrate of the module input", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])
MODULE_OUTPUT_FLOWRATE = Counter("module_output_flowrate", "The flowrate of the module output", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])
MODULE_EXIT_FLOWRATE = Counter("module_exit_flowrate", "The flowrate of the module exit", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])
MODULE_ERROR_FLOWRATE = Counter("module_error_flowrate", "The flowrate of the module errors", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])

MODULE_SUCCESS_TIME = Summary("module_success_time", "Time spent on successful module execution", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])
MODULE_EXIT_TIME = Summary("module_exit_time", "Time spent on module exit", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])
MODULE_ERROR_TIME = Summary("module_error_time", "Time spent on module error", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])

MODULE_WAITING_TIME = Summary("module_waiting_time", "Time spent waiting for a module to execute", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])


MODULE_WAITING_COUNTER = Gauge("module_waiting_counter", "Number of modules waiting to execute", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])
MODULE_PROCESSING_COUNTER = Gauge("module_processing_counter", "Number of modules currently executing", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id", "module_name", "module_id"])

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
    
    def __del__(self) -> None:
        """
        Destructor to remove the lock for the module.
        """
        if id(self) in self._locks:
            del self._locks[id(self)]

    def run(self, dp: DataPackage, dpc: DataPackageController, dpp: DataPackagePhase, parent_module: Optional[DataPackageModule] = None) -> DataPackageModule:
        """
        Wrapper method that executes the module's main logic within a thread-safe context.
        Measures and records the execution time and waiting time.
        """
        
        MODULE_INPUT_FLOWRATE.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).inc()
        
        dpm = DataPackageModule()
        dpm.module_id=self._id
        dpm.module_name=self._name
        dpm.status=Status.RUNNING
        dpm.start_time=0.0
        dpm.end_time=0.0
        dpm.waiting_time=0.0
        dpm.total_time=0.0
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
            dpm.status=Status.WAITING
            MODULE_WAITING_COUNTER.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).inc()
            self._mutex.acquire()
            MODULE_WAITING_COUNTER.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).dec()
            waiting_time = time.time() - start_time
            dpm.waiting_time = waiting_time
            dpm.status=Status.RUNNING
            MODULE_WAITING_TIME.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).observe(waiting_time)
        
        MODULE_PROCESSING_COUNTER.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).inc()
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
                    dpm.status=Status.ERROR
                    err = exception_to_error(te)
                    dpm.error = err
                    dp.errors.append(err)

        if dpm.status == Status.RUNNING:
            dpm.status = Status.SUCCESS

        end_time = time.time()
        dpm.end_time = end_time
        total_time = end_time - start_time
        dpm.total_time = total_time

        if dpm.status == Status.SUCCESS:
            MODULE_SUCCESS_TIME.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).observe(total_time)
            MODULE_OUTPUT_FLOWRATE.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).inc()
        elif dpm.status == Status.EXIT:
            MODULE_EXIT_TIME.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).observe(total_time)
            MODULE_EXIT_FLOWRATE.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).inc()
        elif dpm.status == Status.ERROR:
            MODULE_ERROR_TIME.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).observe(total_time)
            MODULE_ERROR_FLOWRATE.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).inc()
        else:
            try:
                raise ValueError(f"Invalid status {dpm.status} for module {self._name}")
            except ValueError as ve:
                dpm.status = Status.ERROR
                err = exception_to_error(ve)
                dpm.error = err
                dp.errors.append(err)
        
        MODULE_PROCESSING_COUNTER.labels(pipeline_name=dp.pipeline_name, pipeline_id=dp.pipeline_id, pipeline_instance_id=dp.pipeline_instance_id, controller_name=dpc.controller_name, controller_id=dpc.controller_id, phase_name=dpp.phase_name, phase_id=dpp.phase_id, module_name=self._name, module_id=self._id).dec()
        
        if self._use_mutex:
            self._mutex.release()
            
        return dpm

        
        


    def _execute_with_result(self, data: DataPackage, dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
        """
        Helper method to execute the `execute` method and store the result in a container.
        """
        try:
            self.execute(data, dpc, dpp, dpm)
        except Exception as e:
            current_thread = threading.current_thread()
            if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                pipeline_logger = PipelineLogger()
                pipeline_logger.warning(f"Execution of module {self._name} was interrupted due to timeout.")
                return
            dpm.status = Status.ERROR
            err = exception_to_error(e)
            dpm.error = err
            data.errors.append(err)

    @abstractmethod
    def init_module(self) -> None:
        """
        Abstract method to be implemented by subclasses.
        Initializes the module. Will be called by the pipeline when the module is added to the pipeline. Will be called only once.
        """
        pass

    @abstractmethod
    def execute(self, data: DataPackage, data_package_controller: DataPackageController, data_package_phase: DataPackagePhase, data_package_module: DataPackageModule) -> None:
        """
        Abstract method to be implemented by subclasses.
        Performs an operation on the data package.
        """
        pass

    def _deepcopy_lock_aware(self, obj: Any, memo: Dict[int, Any]) -> Any:
        """
        Recursively deep-copies objects, ensuring that any locks found
        (even in nested containers) are replaced with newly created locks.
        """
        # If we already copied this object, return from memo
        obj_id = id(obj)
        if obj_id in memo:
            return memo[obj_id]

        # Handle lock objects explicitly
        if isinstance(obj, LockType):
            new_obj = threading.Lock()
            memo[obj_id] = new_obj
            return new_obj
        if isinstance(obj, RLockType):
            new_obj = threading.RLock()
            memo[obj_id] = new_obj
            return new_obj

        # Handle lists by deep-copying each item
        if isinstance(obj, list):
            new_list = []
            memo[obj_id] = new_list  # put in memo before recursion
            for item in obj:
                new_list.append(self._deepcopy_lock_aware(item, memo))
            return new_list

        # Handle dicts by deep-copying each value
        if isinstance(obj, dict):
            new_dict = {}
            memo[obj_id] = new_dict  # put in memo before recursion
            for k, v in obj.items():
                new_dict[k] = self._deepcopy_lock_aware(v, memo)
            return new_dict

        # For everything else, fall back to standard copy.deepcopy
        # This also recursively uses the same memo.
        new_obj = copy.deepcopy(obj, memo)
        # Put that result into memo so we don't duplicate in future
        memo[obj_id] = new_obj
        return new_obj

    def __deepcopy__(self, memo: Dict[int, Any]) -> Any:
        # If we've seen this object already, return its copy
        if id(self) in memo:
            return memo[id(self)]

        # Create a new, empty instance of the same class
        cls = self.__class__
        copy_obj = cls.__new__(cls)
        memo[id(self)] = copy_obj  # store in memo early

        # Deep-copy every attribute using our custom lock-aware function
        for attr_name, attr_value in self.__dict__.items():
            if not self._deepcopy_skip_attr_name(attr_name):
                setattr(copy_obj, attr_name, self._deepcopy_lock_aware(attr_value, memo))

        return copy_obj
    
    def _deepcopy_skip_attr_name(self, attr_name: str) -> bool:
        """
        Method to determine whether an attribute should be skipped during deep copy. Pls override in subclass.
        """
        return False
    
    def _get_api_annotation(self) -> Dict[Callable[..., Dict[str, Any]], Dict[str, Any]]:
        result: Dict[Callable[..., Dict[str, Any]], Dict[str, Any]] = {}

        for method_name in dir(self):
            method = getattr(self, method_name)
            if hasattr(method, '__dict__') and 'add_route' in method.__dict__.get('__annotations__', {}):
                result[method] = method.__dict__['__annotations__']['add_route']

        return result

class ExecutionModule(Module, ABC):
    def __init__(self, options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)

    def init_module(self) -> None:
        pass

    """
    Abstract class for modules that perform specific execution tasks.
    """
    @abstractmethod
    def execute(self, data: DataPackage, data_package_controller: DataPackageController, data_package_phase: DataPackagePhase, data_package_module: DataPackageModule) -> None:
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

    def init_module(self) -> None:
        self.true_module.init_module()
        self.false_module.init_module()

    @abstractmethod
    def condition(self, data: DataPackage) -> bool:
        """
        Abstract method to be implemented by subclasses to evaluate conditions based on data input.
        """
        return True

    @final
    def execute(self, data: DataPackage, dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
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

    def init_module(self) -> None:
        for module in self.modules:
            module.init_module()
    
    @final
    def execute(self, data: DataPackage, dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
        """
        Executes each module in the list sequentially, passing the output of one as the input to the next.
        """
        for i, module in enumerate(self.modules):
            dpm_sup = module.run(data, dpc, dpp, dpm)
            if not dpm_sup.status == Status.SUCCESS:
                dpm.status = dpm_sup.status
                break


class ExternalModule(Module):
    """
    Class for a module that runs on a different server. Using gRPC for communication.
    """
    def __init__(self, host: str, port: int, options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.host: str = host
        self.port: int = port

    def init_module(self) -> None:
        pass

    def _get_sub_module(self, dpm: DataPackageModule, sub_dp_module_id: str) -> Optional[DataPackageModule]:
        for sub_module in dpm.sub_modules:
            if sub_module.id == sub_dp_module_id:
                return sub_module
            else:
                sub_sub_module = self._get_sub_module(sub_module, sub_dp_module_id)
                if sub_sub_module:
                    return sub_sub_module
        return None

    @final
    def execute(self, data: DataPackage, dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
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
                sub_dpm_id = response.data_package_module_id
                data.set_from_grpc(response.data_package)
                sub_dpm = self._get_sub_module(dpm, sub_dpm_id)
                if not sub_dpm:
                    raise ValueError(f"Sub module with id {sub_dpm_id} not found in parent module.")
                dpm.status = sub_dpm.status