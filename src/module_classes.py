from abc import ABC, abstractmethod
import threading
from typing import Any, List, Tuple, final
import time
from prometheus_client import Summary

# Create a metric to track time spent and requests made.
REQUEST_PROCESSING_TIME = Summary('module_processing_seconds', 'Time spent processing module', ['module_name'])
REQUEST_WAITING_TIME = Summary('module_waiting_seconds', 'Time spent waiting bevore executing the task (mutex)', ['module_name'])
REQUEST_TOTAL_TIME = Summary('module_total_seconds', 'Time spent processing module', ['module_name'])

class Module(ABC):
    _locks = {}
    
    @property
    def mutex(self):
        if id(self) not in self._locks:
            self._locks[id(self)] = threading.RLock()  # Using RLock to allow reentrant locking
        return self._locks[id(self)]

    @final
    def run(self, data) -> Tuple[bool, str, Any]:
        start_total_time = time.time()
        self.mutex.acquire()
        REQUEST_WAITING_TIME.labels(module_name=self.__class__.__name__).observe(time.time() - start_total_time)

        module_name = self.__class__.__name__
        start_time = time.time()
        try:
            result = self.execute(data)
            REQUEST_PROCESSING_TIME.labels(module_name=module_name).observe(time.time() - start_time)
            return result
        except Exception as e:
            REQUEST_PROCESSING_TIME.labels(module_name=module_name).observe(time.time() - start_time)
            raise e
        finally:
            self.mutex.release()
            REQUEST_TOTAL_TIME.labels(module_name=module_name).observe(time.time() - start_total_time)

    @abstractmethod
    def execute(self, data) -> Tuple[bool, str, Any]:
        """
        This method should be overridden by subclasses.
        It should perform an operation on the data input and return a tuple (bool, str, Any).
        """
        pass

class ExecutionModule(Module):
    @abstractmethod
    def execute(self, data) -> Tuple[bool, str, Any]:
        """
        This abstract method should be implemented by subclasses to execute specific code.
        """
        return True, "", data

class ConditionModule(Module):
    @final
    def __init__(self, true_module: Module, false_module: Module):
        self.true_module = true_module
        self.false_module = false_module

    @abstractmethod
    def condition(self, data) -> bool:
        """
        This abstract method should be implemented by subclasses to evaluate conditions based on the data input.
        """
        return True

    @final
    def execute(self, data) -> Tuple[bool, str, Any]:
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
    @final
    def __init__(self, modules: List[Module]):
        self.modules = modules
    
    @final
    def execute(self, data) -> Tuple[bool, str, Any]:
        result_data = data
        for i, module in enumerate(self.modules):
            start_time = time.time()
            try:
                result, result_message, result_data = module.run(result_data)
                if not result:
                    return False, result_message, result_data
            except Exception as e:
                raise Exception(f"Combination module {i} ({module.__class__.__name__}) failed with error: {str(e)} and data: {result_data}")
        return True, "", result_data
