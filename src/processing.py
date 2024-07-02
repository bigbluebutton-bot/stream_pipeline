from .module_classes import Module, ExecutionModule, ConditionModule, CombinationModule

class Processing:
    def __init__(self, modules: list):
        for module in modules:
            if not isinstance(module, Module):
                raise TypeError(f"Module {module} is not a subclass of Module")
        self.modules = modules

    def execute(self, data):
        result_data = data
        for i, module in enumerate(self.modules):
            try:
                result, result_message, result_data = module.execute(result_data)
                if not result:
                    return False, f"Module {i} ({module.__class__.__name__}) failed: {result_message}", result_data
            except Exception as e:
                return False, f"Module {i} ({module.__class__.__name__}) failed with error: {str(e)}", result_data
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
