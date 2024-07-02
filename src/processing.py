import inspect
import time
from typing import List, Tuple, Any
from .module_classes import Module, ExecutionModule, ConditionModule, CombinationModule

class Processing:
    """
    Class to manage and execute a sequence of modules.
    """
    def __init__(self, modules: List[Module]):
        for module in modules:
            if not isinstance(module, Module):
                raise TypeError(f"Module {module} is not a subclass of Module")
            self._validate_execute_method(module)
        self.modules = modules

    def _validate_execute_method(self, module) -> None:
        """
        Validates that the module has an execute method with the correct signature.
        """
        execute_method = getattr(module, 'execute', None)
        if execute_method is None:
            raise TypeError(f"Module {module.__class__.__name__} does not have an 'execute' method")

        # Check the method signature
        signature = inspect.signature(execute_method)
        parameters = list(signature.parameters.values())
        if len(parameters) != 1 and parameters[0].name != 'data':
            raise TypeError(f"'execute' method of {module.__class__.__name__} must accept exactly one parameter 'data'")

    def run(self, data) -> Tuple[bool, str, Any]:
        """
        Runs the sequence of modules on the given data.
        """
        result_data = data
        for i, module in enumerate(self.modules):
            module_name = module.__class__.__name__
            try:
                result = module.run(result_data)

                if not (isinstance(result, tuple) and len(result) == 3 and isinstance(result[0], bool) and isinstance(result[1], str)):
                    raise TypeError(f"Module {i} ({module_name}) returned an invalid result. Expected (bool, str, Any). Got {result}")

                result, result_message, result_data = result
                if not result:
                    return False, f"Module {i} ({module_name}) failed: {result_message}", result_data
            except Exception as e:
                return False, f"Module {i} ({module_name}) failed with error: {str(e)}", result_data
        return True, "Processing succeeded", result_data

class ProcessingManager:
    """
    Class to manage pre-processing, main processing, and post-processing stages.
    """
    def __init__(self, pre_modules: list, main_modules: list, post_modules: list):
        self.pre_processing = Processing(pre_modules)
        self.main_processing = Processing(main_modules)
        self.post_processing = Processing(post_modules)

    def run(self, data) -> Tuple[bool, str, Any]:
        """
        Executes the pre-processing, main processing, and post-processing stages sequentially.
        """
        pre_result, pre_message, pre_data = self.pre_processing.run(data)
        if not pre_result:
            return False, f"Pre-processing failed: {pre_message}", pre_data

        main_result, main_message, main_data = self.main_processing.run(pre_data)
        if not main_result:
            return False, f"Main processing failed: {main_message}", main_data

        post_result, post_message, post_data = self.post_processing.run(main_data)
        if not post_result:
            return False, f"Post-processing failed: {post_message}", post_data

        return True, "All processing succeeded", post_data