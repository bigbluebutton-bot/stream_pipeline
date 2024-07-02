from abc import ABC, abstractmethod
from typing import final
import time
from prometheus_client import Summary

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('module_processing_seconds', 'Time spent processing module', ['module_name'])


class Module(ABC):
    @abstractmethod
    def execute(self, data):
        """
        This method should be overridden by subclasses.
        It should perform an operation on the data input and return a tuple (bool, data).
        """
        return True, "", data

class ExecutionModule(Module):
    @abstractmethod
    def execute(self, data):
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
    def condition(self, data):
        """
        This abstract method should be implemented by subclasses to evaluate conditions based on the data input.
        """
        return True

    @final
    def execute(self, data):
        if self.condition(data):
            try:
                return self.true_module.execute(data)
            except Exception as e:
                raise Exception(f"True module failed with error: {str(e)}")
        else:
            try:
                return self.false_module.execute(data)
            except Exception as e:
                raise Exception(f"False module failed with error: {str(e)}")

class CombinationModule(Module):
    @final
    def __init__(self, modules: list):
        self.modules = modules
    
    @final
    def execute(self, data):
        result_data = data
        for i, module in enumerate(self.modules):
            module_name = module.__class__.__name__
            start_time = time.time()
            try:
                result, result_message, result_data = module.execute(result_data)
                duration = time.time() - start_time
                if module_name is not "CombinationModule":
                    REQUEST_TIME.labels(module_name=module_name).observe(duration)
                if not result:
                    return False, result_message, result_data
            except Exception as e:
                duration = time.time() - start_time
                if module_name is not "CombinationModule":
                    REQUEST_TIME.labels(module_name=module_name).observe(duration)
                return False, f"Combination module {i} ({module.__class__.__name__}) failed with error: {str(e)}", result_data
        return True, "", result_data