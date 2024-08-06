# main.py

from typing import List


def main() -> None:
    import random
    import threading
    from typing import Union
    from stream_pipeline.data_package import DataPackageModule
    from stream_pipeline.module_classes import ExecutionModule, ConditionModule, CombinationModule, Module, ModuleOptions, DataPackage, ExternalModule
    from stream_pipeline.pipeline import Pipeline, ControllerMode, PipelinePhase, PipelineController
    from prometheus_client import start_http_server
    import time
    import stream_pipeline.error as error

    err_logger = error.ErrorLogger()
    err_logger.set_debug(True)


    # Start up the server to expose the metrics.
    start_http_server(8000)

    # Creating a data type
    class Data:
        def __init__(self, key: str, condition: bool) -> None:
            self.key = key
            self.condition = condition
            self.status = "unknown"
            
        def __str__(self) -> str:
            return f"Data: {self.key}, {self.condition}"


    # Example custom modules
    class DataValidationModule(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpm: DataPackageModule) -> None:
            if dp.data and dp.data.key:
                dp.success = True
                dpm.message = "Validation succeeded"
            else:
                raise ValueError("Validation failed: key missing")

    class DataTransformationModule(ExecutionModule):
        def __init__(self) -> None:
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=40.0
            ))

        def execute(self, dp: DataPackage[Data], dpm: DataPackageModule) -> None:
            list1 = [1, 2, 3, 4, 5, 6]
            randomint = random.choice(list1)
            time.sleep(randomint)
            if dp.data:
                if dp.data.key:
                    dp.data.key = dp.data.key.upper()
                    dp.success = True
                    dpm.message = "Transformation succeeded"
                else:
                    dp.success = False
                    dpm.message = "Transformation failed: key missing"

    class DataConditionModule(ConditionModule):
        def condition(self, dp: DataPackage[Data]) -> bool:
            if dp.data:
                return dp.data.condition == True
            return False

    class SuccessModule(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpm: DataPackageModule) -> None:
            if dp.data:
                dp.data.status = "success"
                dp.success = True
                dpm.message = "Condition true: success"

    class FailureModule(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpm: DataPackageModule) -> None:
            if dp.data:
                dp.data.status = "failure"
                dp.success = True
                dpm.message = "Condition false: failure"

    class AlwaysTrue(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpm: DataPackageModule) -> None:
            dp.success = True
            dpm.message = "Always true"

    # Setting up the processing pipeline
    phases = [
        PipelineController(
            mode=ControllerMode.ORDER_BY_SEQUENCE,
            max_workers=10,
            name="phase1",
            phases=[
                PipelinePhase([
                    DataValidationModule(),
                ]),
            ],
        ),
        PipelineController(
            mode=ControllerMode.NOT_PARALLEL,
            max_workers=10,
            name="phase2",
            phases=[
                PipelinePhase([
                    DataConditionModule(SuccessModule(), FailureModule()),
                    AlwaysTrue(),
                ]),
            ],
        ),
        PipelineController(
            mode=ControllerMode.NO_ORDER,
            max_workers=10,
            name="phase3",
            phases=[
                PipelinePhase([
                    CombinationModule([
                        CombinationModule([
                            DataTransformationModule(),
                            # ExternalModule("localhost", 50051, ModuleOptions(use_mutex=False)),
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

    pipeline = Pipeline[Data](phases, "test-pipeline")
    pip_ex_id = pipeline.register_instance()

    counter = 0
    counter_mutex = threading.Lock()
    def callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        print(f"OK: {dp.data}")
        with counter_mutex:
            counter = counter + 1

    def error_callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        print(f"ERROR: {dp}")
        with counter_mutex:
            counter = counter + 1

    # Function to execute the processing pipeline
    def process_data(data: Data) -> Union[DataPackage, None]:
        return pipeline.execute(data, pip_ex_id, callback, error_callback)

    # Example data
    data_list: List[Data] = [
        Data(key="value0", condition=True),
        Data(key="value1", condition=False),
        Data(key="value2", condition=True),
        Data(key="value3", condition=False),
        Data(key="value4", condition=True),
        Data(key="value5", condition=False),
        Data(key="value6", condition=True),
        Data(key="value7", condition=False),
        Data(key="value8", condition=True),
        Data(key="value9", condition=False),
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