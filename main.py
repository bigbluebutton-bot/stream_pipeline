from src.module_classes import ExecutionModule, ConditionModule
from src.processing import ProcessingManager

# Example custom modules
class DataValidationModule(ExecutionModule):
    def execute(self, data):
        if isinstance(data, dict) and "key" in data:
            return True, "Validation succeeded", data
        return False, "Validation failed: key missing", data

class DataTransformationModule(ExecutionModule):
    def execute(self, data):
        if "key" in data:
            data["key"] = data["key"].upper()
            return True, "Transformation succeeded", data
        return False, "Transformation failed: key missing", data

class DataConditionModule(ConditionModule):
    def condition(self, data):
        return "condition" in data and data["condition"] == True

class SuccessModule(ExecutionModule):
    def execute(self, data):
        data["status"] = "success"
        return True, "Condition true: success", data

class FailureModule(ExecutionModule):
    def execute(self, data):
        data["status"] = "failure"
        return True, "Condition false: failure", data

# Setting up the processing pipeline
pre_modules = [DataValidationModule()]
main_modules = [
    DataTransformationModule(),
    DataConditionModule(SuccessModule(), FailureModule())
]
post_modules = []

manager = ProcessingManager(pre_modules, main_modules, post_modules)

# Example data
data = {"key": "value", "condition": False}

# Execute the processing pipeline
result, message, processed_data = manager.execute(data)
print(result, message, processed_data)