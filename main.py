# main.py

def main():
    import random
    import threading
    from typing import Union
    from stream_pipeline.data_package import DataPackageModule
    from stream_pipeline.module_classes import ExecutionModule, ConditionModule, CombinationModule, Module, ModuleOptions, DataPackage, ExternalModule
    from stream_pipeline.pipeline import Pipeline, PhaseExecutionMode, PipelinePhase, PipelinePhaseExecution
    from prometheus_client import start_http_server
    import time
    import stream_pipeline.error as error

    err_logger = error.ErrorLogger()
    err_logger.set_debug(True)


    # Start up the server to expose the metrics.
    start_http_server(8000)

    # Example custom modules
    class DataValidationModule(ExecutionModule):
        def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
            if isinstance(data.data, dict) and "key" in data.data:
                data.success = True
                dpm.message = "Validation succeeded"
            else:
                data.success = False
                dpm.message = "Validation failed: key missing"

    class DataTransformationModule(ExecutionModule):
        def __init__(self):
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=40.0
            ))

        def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
            list1 = [1, 2, 3, 4, 5, 6]
            randomint = random.choice(list1)
            time.sleep(randomint)
            if "key" in data.data:
                data.data["key"] = data.data["key"].upper()
                data.success = True
                dpm.message = "Transformation succeeded"
            else:
                data.success = False
                dpm.message = "Transformation failed: key missing"

    class DataConditionModule(ConditionModule):
        def condition(self, data: DataPackage) -> bool:
            return "condition" in data.data and data.data["condition"] == True

    class SuccessModule(ExecutionModule):
        def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
            data.data["status"] = "success"
            data.success = True
            dpm.message = "Condition true: success"

    class FailureModule(ExecutionModule):
        def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
            data.data["status"] = "failure"
            data.success = True
            dpm.message = "Condition false: failure"

    class AlwaysTrue(ExecutionModule):
        def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
            data.success = True
            dpm.message = "Always true"

    # Setting up the processing pipeline

    phases = [
        PipelinePhaseExecution(
            mode=PhaseExecutionMode.ORDER_BY_SEQUENCE,
            max_workers=10,
            name="phase1",
            phases=[
                PipelinePhase([
                    DataValidationModule(),
                ]),
            ],
        ),
        PipelinePhaseExecution(
            mode=PhaseExecutionMode.NOT_PARALLEL,
            max_workers=10,
            name="phase2",
            phases=[
                PipelinePhase([
                    DataConditionModule(SuccessModule(), FailureModule()),
                    AlwaysTrue(),
                ]),
            ],
        ),
        PipelinePhaseExecution(
            mode=PhaseExecutionMode.NO_ORDER,
            max_workers=10,
            name="phase3",
            phases=[
                PipelinePhase([
                    CombinationModule([
                        CombinationModule([
                            DataTransformationModule(),
                            ExternalModule("localhost", 50051, ModuleOptions(use_mutex=False)),
                        ], ModuleOptions(
                            use_mutex=False,
                        )),
                    ], ModuleOptions(
                            use_mutex=False,
                        ))
                ]),
            ],
        ),
    ]

    pipeline = Pipeline(phases, "test-pipeline")
    pip_ex_id = pipeline.register_instance()

    counter = 0
    counter_mutex = threading.Lock()
    def callback(processed_data: DataPackage):
        nonlocal counter, counter_mutex
        print(f"OK: {processed_data.data}")
        with counter_mutex:
            counter = counter + 1

    def error_callback(processed_data: DataPackage):
        nonlocal counter, counter_mutex
        print(f"ERROR: {processed_data}")
        with counter_mutex:
            counter = counter + 1

    # Function to execute the processing pipeline
    def process_data(data) -> Union[DataPackage, None]:
        return pipeline.execute(data, pip_ex_id, callback, error_callback)

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
    dp: Union[DataPackage, None] = None
    for d in data_list:
        dp = process_data(d)

    # Keep the main thread alive
    while True:
        time.sleep(0.001)
        if counter >= len(data_list):
            break

    pipeline.unregister_instance(pip_ex_id)



    print(f"Example DataPackage: {dp}")
    print("THE END")

if __name__ == "__main__":
    main()