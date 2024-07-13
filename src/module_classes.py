# module_classes.py

from abc import ABC, abstractmethod
import threading
from typing import Any, Dict, List, Tuple, final, NamedTuple
import time
import uuid
from prometheus_client import Gauge, Summary
from .data_package import DataPackage

# Metrics to track time spent on processing modules
REQUEST_PROCESSING_TIME = Summary('module_processing_seconds', 'Time spent processing module', ['module_name'])
REQUEST_PROCESSING_COUNTER = Gauge('module_processing_counter', 'Number of processes executing the module at the moment', ['module_name'])
REQUEST_WAITING_TIME = Summary('module_waiting_seconds', 'Time spent waiting before executing the task (mutex)', ['module_name'])
REQUEST_TOTAL_TIME = Summary('module_total_seconds', 'Total time spent processing module', ['module_name'])
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
    _locks = {}

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

    @final
    def run(self, data) -> Tuple[bool, str, Any]:
        """
        Wrapper method that executes the module's main logic within a thread-safe context.
        Measures and records the execution time and waiting time.
        """
        start_total_time = time.time()
        if self._use_mutex:
            REQUEST_WAITING_COUNTER.labels(module_name=self.__class__.__name__).inc()
            self._mutex.acquire()
            REQUEST_WAITING_TIME.labels(module_name=self.__class__.__name__).observe(time.time() - start_total_time)
            REQUEST_WAITING_COUNTER.labels(module_name=self.__class__.__name__).dec()

        start_time = time.time()
        
        # Create a thread to execute the execute method
        result_container: Dict[str, (Tuple[bool, str, Any])]  = {}
        execute_thread = threading.Thread(target=self._execute_with_result, args=(data, result_container))
        execute_thread.start()
        execute_thread.join(self._timeout)

        if execute_thread.is_alive():
            if self._use_mutex:
                self._mutex.release()
            raise TimeoutError(f"Execution of module {self._name} timed out after {self._timeout} seconds.")

        success, message, data = result_container['result']
        REQUEST_PROCESSING_TIME.labels(module_name=self.__class__.__name__).observe(time.time() - start_time)
        
        if not success:
            if self._use_mutex:
                self._mutex.release()
            return False, message, data
        
        if self._use_mutex:
            self._mutex.release()
        REQUEST_TOTAL_TIME.labels(module_name=self.__class__.__name__).observe(time.time() - start_total_time)
        
        return success, message, data


    def _execute_with_result(self, data, result_container):
        """
        Helper method to execute the `execute` method and store the result in a container.
        """
        REQUEST_PROCESSING_COUNTER.labels(module_name=self.__class__.__name__).inc()
        try:
            result_container['result'] = self.execute(data)
        except Exception as e:
            result_container['result'] = (False, str(e), None)
        finally:
            REQUEST_PROCESSING_COUNTER.labels(module_name=self.__class__.__name__).dec()

    @abstractmethod
    def execute(self, data) -> Tuple[bool, str, Any]:
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
    def execute(self, data) -> Tuple[bool, str, Any]:
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
    def execute(self, data) -> Tuple[bool, str, Any]:
        """
        Executes the true_module if condition is met, otherwise executes the false_module.
        """
        if self.condition(data):
            try:
                return self.true_module.run(data)
            except Exception as e:
                raise Exception(f"True module failed with error: {str(e)}")
        else:
            try:
                return self.false_module.run(data)
            except Exception as e:
                raise Exception(f"False module failed with error: {str(e)}")

class CombinationModule(Module):
    """
    Class for modules that combine multiple modules sequentially.
    """
    @final
    def __init__(self, modules: List[Module], options: ModuleOptions = ModuleOptions(), name: str = ""):
        super().__init__(options, name)
        self.modules = modules
    
    @final
    def execute(self, data) -> Tuple[bool, str, Any]:
        """
        Executes each module in the list sequentially, passing the output of one as the input to the next.
        """
        result_data = data
        for i, module in enumerate(self.modules):
            try:
                result, result_message, result_data = module.run(result_data)
                if not result:
                    return False, result_message, result_data
            except Exception as e:
                raise Exception(f"Combination module {i} ({module.__class__.__name__}) failed with error: {str(e)} and data: {result_data}")
        return True, "", result_data
