# module_classes.py

from abc import ABC, abstractmethod
import threading
from typing import  Dict, List, Tuple, Union, final, NamedTuple
import time
import uuid
import grpc # type: ignore
from prometheus_client import Gauge, Summary

from . import data_pb2
from . import data_pb2_grpc

from .data_package import DataPackage, DataPackageModule

# Metrics to track time spent on processing modules
REQUEST_PROCESSING_TIME = Summary('module_processing_seconds', 'Time spent processing module', ['module_name'])
REQUEST_PROCESSING_TIME_WITHOUT_ERROR = Summary('module_processing_seconds_without_error', 'Time spent processing module without error', ['module_name'])
REQUEST_PROCESSING_COUNTER = Gauge('module_processing_counter', 'Number of processes executing the module at the moment', ['module_name'])
REQUEST_WAITING_TIME = Summary('module_waiting_seconds', 'Time spent waiting before executing the task (mutex)', ['module_name'])
REQUEST_TOTAL_TIME = Summary('module_total_seconds', 'Total time spent processing module', ['module_name'])
REQUEST_TOTAL_TIME_WITHOUT_ERROR = Summary('module_total_seconds_without_error', 'Total time spent processing module without error', ['module_name'])
REQUEST_WAITING_COUNTER = Gauge('module_waiting_counter', 'Number of processes waiting to execute the task (mutex)', ['module_name'])

class ModuleOptions(NamedTuple):
    """
    Named tuple to store options for modules.
    Attributes:
        use_mutex (bool): whether to use a mutex lock for thread safety (Default: True)
        timeout (float): timeout to stop executing after x seconds. If 0.0, waits indefinitely (Default: 0.0)
    """
    use_mutex: bool = True
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

    def get_id(self):
        return self._id
    
    def get_name(self):
        return self._name

    @property
    def _mutex(self):
        """
        Provides a reentrant lock for each module instance.
        """
        if id(self) not in self._locks:
            self._locks[id(self)] = threading.RLock()
        return self._locks[id(self)]

    def run(self, data_package: DataPackage) -> None:
        """
        Wrapper method that executes the module's main logic within a thread-safe context.
        Measures and records the execution time and waiting time.
        """
        dpm = DataPackageModule(
                module_id=self._id,
                running=True,
                start_time=0.0,
                end_time=0.0,
                waiting_time=0.0,
                processing_time=0.0,
                total_time=0.0,
                success=True,
                error=None,
            )
        
        data_package.modules.append(dpm)
        
        start_total_time = time.time()
        waiting_time = 0.0
        if self._use_mutex:
            REQUEST_WAITING_COUNTER.labels(module_name=self.__class__.__name__).inc()
            self._mutex.acquire()
            waiting_time = time.time() - start_total_time
            dpm.waiting_time = waiting_time
            REQUEST_WAITING_TIME.labels(module_name=self.__class__.__name__).observe(waiting_time)
            REQUEST_WAITING_COUNTER.labels(module_name=self.__class__.__name__).dec()

        start_time = time.time()
        dpm.start_time = start_total_time
        
        # Create a thread to execute the execute method
        execute_thread = threading.Thread(target=self._execute_with_result, args=(data_package, dpm))
        execute_thread.start_context = threading.current_thread().name # type: ignore
        execute_thread.timed_out = False # type: ignore
        REQUEST_PROCESSING_COUNTER.labels(module_name=self.__class__.__name__).inc()
        execute_thread.start()
        execute_thread.join(self._timeout)
        execute_thread.timed_out = True # type: ignore
        REQUEST_PROCESSING_COUNTER.labels(module_name=self.__class__.__name__).dec()

        thread_alive = execute_thread.is_alive()
        if thread_alive:
            if thread_alive:
                try:
                    # Raise a TimeoutError to stop the thread
                    raise TimeoutError(f"Execution of module {self._name} timed out after {self._timeout} seconds.")
                except TimeoutError as te:
                    dpm.success = False
                    dpm.error = te

        if not dpm.success:
            data_package.success = False
            data_package.errors.append(dpm.error)
        
        end_time = time.time()
        dpm.end_time = end_time
        processing_time = end_time - start_time
        dpm.processing_time = processing_time
        total_time = end_time - start_total_time
        dpm.total_time = total_time
        dpm.running = False


        if data_package.success:
            REQUEST_PROCESSING_TIME_WITHOUT_ERROR.labels(module_name=self.__class__.__name__).observe(processing_time)
            REQUEST_TOTAL_TIME_WITHOUT_ERROR.labels(module_name=self.__class__.__name__).observe(total_time)
        
        REQUEST_PROCESSING_TIME.labels(module_name=self.__class__.__name__).observe(processing_time)
        REQUEST_TOTAL_TIME.labels(module_name=self.__class__.__name__).observe(total_time)
        
        if self._use_mutex:
            self._mutex.release()

        
        


    def _execute_with_result(self, data: DataPackage, dpm: DataPackageModule) -> None:
        """
        Helper method to execute the `execute` method and store the result in a container.
        """
        try:
            self.execute(data)
        except Exception as e:
            current_thread = threading.current_thread()
            if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                # print(f"WARNING: Execution of module {self._name} was interrupted due to timeout.")
                return
            dpm.success = False
            dpm.error = e

    @abstractmethod
    def execute(self, data: DataPackage) -> None:
        """
        Abstract method to be implemented by subclasses.
        Performs an operation on the data input and returns a tuple (bool, str, Any).
        """
        pass

class ExecutionModule(Module, ABC):
    def __init__(self, options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)

    """
    Abstract class for modules that perform specific execution tasks.
    """
    @abstractmethod
    def execute(self, data: DataPackage) -> None:
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
    def condition(self, data) -> bool:
        """
        Abstract method to be implemented by subclasses to evaluate conditions based on data input.
        """
        return True

    @final
    def execute(self, data: DataPackage) -> None:
        """
        Executes the true_module if condition is met, otherwise executes the false_module.
        """
        if self.condition(data):
            try:
                self.true_module.run(data)
            except Exception as e:
                # Create a new error instance of the same type with additional information
                new_error = type(e)(f"True module failed with error: {str(e)}")
                data.errors.append(new_error)
        else:
            try:
                self.false_module.run(data)
            except Exception as e:
                # Create a new error instance of the same type with additional information
                new_error = type(e)(f"False module failed with error: {str(e)}")
                data.errors.append(new_error)

class CombinationModule(Module):
    """
    Class for modules that combine multiple modules sequentially.
    """
    @final
    def __init__(self, modules: List[Module], options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.modules = modules
    
    @final
    def execute(self, data: DataPackage) -> None:
        """
        Executes each module in the list sequentially, passing the output of one as the input to the next.
        """
        result_data = data
        for i, module in enumerate(self.modules):
            try:
                module.run(result_data)
                if not result_data.success:
                    break
            except Exception as e:
                # Create a new error instance of the same type with additional information
                new_error = type(e)(f"Combination module {i} ({module.__class__.__name__}) failed with error: {str(e)}")
                data.errors.append(new_error)
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
    def execute(self, data: DataPackage) -> None:
        address = f"{self.host}:{self.port}"
        with grpc.insecure_channel(address) as channel:
            stub = data_pb2_grpc.ModuleServiceStub(channel)
            data_grpc = data.to_grpc()
            response = stub.run(data_grpc)

            data.set_from_grpc(response)