import inspect
import time
from .module_classes import Module, ExecutionModule, ConditionModule, CombinationModule, REQUEST_TIME

class Processing:
    def __init__(self, modules: list):
        for module in modules:
            if not isinstance(module, Module):
                raise TypeError(f"Module {module} is not a subclass of Module")
            self._validate_execute_method(module)
        self.modules = modules

    def _validate_execute_method(self, module):
        execute_method = getattr(module, 'execute', None)
        if execute_method is None:
            raise TypeError(f"Module {module.__class__.__name__} does not have an 'execute' method")

        # Check the method signature
        signature = inspect.signature(execute_method)
        parameters = list(signature.parameters.values())
        if len(parameters) != 1 and parameters[1].name != 'data':
            raise TypeError(f"'execute' method of {module.__class__.__name__} must accept exactly one parameter 'data'")

    def execute(self, data):
        result_data = data
        for i, module in enumerate(self.modules):
            module_name = module.__class__.__name__
            start_time = time.time()
            try:
                result = module.execute(result_data)
                duration = time.time() - start_time
                if module_name is not "CombinationModule":
                    REQUEST_TIME.labels(module_name=module_name).observe(duration)

                if not (isinstance(result, tuple) and len(result) == 3 and isinstance(result[0], bool) and isinstance(result[1], str)):
                    raise TypeError(f"Module {i} ({module_name}) returned an invalid result. Expected (bool, str, any). Got {result}")

                result, result_message, result_data = result
                if not result:
                    return False, f"Module {i} ({module_name}) failed: {result_message}", result_data
            except Exception as e:
                duration = time.time() - start_time
                if module_name is not "CombinationModule":
                    REQUEST_TIME.labels(module_name=module_name).observe(duration)
                return False, f"Module {i} ({module_name}) failed with error: {str(e)}", result_data
        return True, "Processing succeeded", result_data

class ProcessingManager:
    def __init__(self, pre_modules: list, main_modules: list, post_modules: list):
        self.pre_processing = Processing(pre_modules)
        self.main_processing = Processing(main_modules)
        self.post_processing = Processing(post_modules)

    def execute(self, data):
        pre_result, pre_message, pre_data = self.pre_processing.execute(data)
        if not pre_result:
            return False, f"Pre-processing failed: {pre_message}", pre_data

        main_result, main_message, main_data = self.main_processing.execute(pre_data)
        if not main_result:
            return False, f"Main processing failed: {main_message}", main_data

        post_result, post_message, post_data = self.post_processing.execute(main_data)
        if not post_result:
            return False, f"Post-processing failed: {post_message}", post_data

        return True, "All processing succeeded", post_data