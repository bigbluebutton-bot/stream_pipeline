# main.py
import random
from src.module_classes import ExecutionModule, ConditionModule, CombinationModule, ModuleOptions
from pipeline import Pipeline
from prometheus_client import start_http_server
import concurrent.futures
import time

# Start up the server to expose the metrics.
start_http_server(8000)

# Example custom modules
class DataValidationModule(ExecutionModule):
    def execute(self, data):
        if isinstance(data, dict) and "key" in data:
            return True, "Validation succeeded", data
        return False, "Validation failed: key missing", data

class DataTransformationModule(ExecutionModule):
    def __init__(self):
        super().__init__(ModuleOptions(
            use_mutex=False,
        ))

    def execute(self, data):
        list1 = [1, 2, 3, 4, 5, 6]
        randomint = random.choice(list1)
        time.sleep(randomint)
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

class AlwaysTrue(ExecutionModule):
    def execute(self, data):
        return True, "Always true", data

# Setting up the processing pipeline
pre_modules = [DataValidationModule()]
main_modules = [
    DataTransformationModule(),
    DataConditionModule(SuccessModule(), FailureModule())
]
post_modules = [
    CombinationModule([
        CombinationModule([
            AlwaysTrue(),
        ]),
    ])
]

manager = Pipeline("test-pipeline", pre_modules, main_modules, post_modules)

def callback(result, message, processed_data):
    print(result, message, processed_data)

# Function to execute the processing pipeline
def process_data(data):
    manager.run(data, callback)

# Example data
data_list = [
    {"key": "value0", "condition": True},
    {"key": "value1", "condition": False},
    {"key": "value2", "condition": True},
    {"key": "value3", "condition": False},
    {"key": "value4", "condition": True},
    {"key": "value5", "condition": False},
    {"key": "value6", "condition": True},
    {"key": "value7", "condition": False},
    {"key": "value8", "condition": True},
    {"key": "value9", "condition": False},
]

for d in data_list:
    process_data(d)

# # Using ThreadPoolExecutor for multithreading
# with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
#     futures = [executor.submit(process_data, data) for data in data_list]


# Keep the main thread alive
while True:
    time.sleep(1)
