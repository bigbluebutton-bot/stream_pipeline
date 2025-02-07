
import random
import threading
from typing import Dict, Union, List

from pydantic import BaseModel
from stream_pipeline.data_package import DataPackageController, DataPackagePhase, DataPackageModule, Status
from stream_pipeline.module_classes import ExecutionModule, ConditionModule, CombinationModule, Module, ModuleOptions, DataPackage, ExternalModule
from stream_pipeline.pipeline import Pipeline, ControllerMode, PipelinePhase, PipelineController
from stream_pipeline.logger import PipelineLogger, format_json
from prometheus_client import start_http_server
from stream_pipeline.api import APIService, add_route
import time
import json

from data import Data

def main() -> None:
    # You can set your own logging for internal pipeline logging if you want. If not set nothing will be logged.
    pipeline_logger = PipelineLogger()
    pipeline_logger.set_debug(True)
    pipeline_logger.set_info(print)
    pipeline_logger.set_warning(print)
    pipeline_logger.set_error(print)
    pipeline_logger.set_critical(print)
    pipeline_logger.set_log(print)
    pipeline_logger.set_exception(print)
    pipeline_logger.set_excepthook(lambda ex: print(f"{format_json(ex)}"))
    pipeline_logger.set_threading_excepthook(lambda ex: print(f"{format_json(ex)}"))



    # Start up the server to expose the metrics.
    start_http_server(8000)


    class Numbers(BaseModel):
        a: float
        b: float

    class Result(BaseModel):
        result: float

    # Example custom modules
    class DataValidationModule(ExecutionModule):
        def __init__(self) -> None:
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=10.0
            ))
            self.counter = 0

        @add_route(
            sub_path="/data-validation",
            methods=["GET"],
            summary="Data Validation",
            description="Validate data key."
        )
        def api_counter(self) -> Dict[str, str]:
            """Returns the number of times the module has been executed."""
            return {"counter": f"{self.counter}"}
        
        @add_route(
            sub_path="/calculate",
            methods=["POST"],
            summary="Data Validation",
            description="Validate data key."
        )
        def api_calculate(self, numbers: Numbers) -> Result:
            c = numbers.a + numbers.b
            return Result(result=c)

        def execute(self, dp: DataPackage[Data], dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
            self.counter += 1
            if dp.data and dp.data.key:
                dpm.message = "Validation succeeded"
            else:
                raise ValueError("Validation failed: key missing")

    class DataTransformationModule(ExecutionModule):
        def __init__(self) -> None:
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=40.0
            ))

        def execute(self, dp: DataPackage[Data], dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
            list1 = [1, 2, 3, 4, 5, 6]
            randomint = random.choice(list1)
            time.sleep(randomint)
            if dp.data:
                if dp.data.key:
                    dp.data.key = dp.data.key.upper()
                    dpm.message = "Transformation succeeded"
                else:
                    dpm.status = Status.EXIT
                    dpm.message = "Transformation failed: key missing"

    class DataConditionModule(ConditionModule):
        def condition(self, dp: DataPackage[Data]) -> bool:
            if dp.data:
                return dp.data.condition == True
            return False

    class SuccessModule(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
            if dp.data:
                dp.data.status = "success"
                dpm.message = "Condition true: success"

    class FailureModule(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
            if dp.data:
                dp.data.status = "failure"
                dpm.message = "Condition false: failure"

    class RandomExit(ExecutionModule):
        def execute(self, dp: DataPackage[Data], dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
            list1 = [True, True, True, True, True, False]
            randombool = random.choice(list1)
            if randombool:
                dpm.message = "Random exit: success"
            else:
                dpm.status = Status.EXIT
                dpm.message = "Random exit: failure"

    # Setting up the processing pipeline
    controller = [
        PipelineController(
            mode=ControllerMode.ORDER_BY_SEQUENCE,
            max_workers=10,
            name="controller1",
            phases=[
                PipelinePhase(
                    name="c1-phase1",
                    modules=[
                    DataValidationModule(),
                ]),
            ],
        ),
        PipelineController(
            mode=ControllerMode.NOT_PARALLEL,
            max_workers=10,
            name="controller2",
            phases=[
                PipelinePhase(
                    name="c2-phase1",
                    modules=[
                    DataConditionModule(SuccessModule(), FailureModule()),
                ]),
            ],
        ),
        PipelineController(
            mode=ControllerMode.FIRST_WINS,
            max_workers=4,
            queue_size=2,
            name="controller3",
            phases=[
                PipelinePhase(
                    name="c3-phase1",
                    modules=[
                    CombinationModule([
                        CombinationModule([
                            RandomExit(),
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

    pipeline = Pipeline[Data](name="test-pipeline", controllers_or_phases=controller)
    pip_ex_id = pipeline.register_instance()

    api = APIService("0.0.0.0", 8001, pipeline)
    api.run()
    print(api._app.routes)

    counter = 0
    counter_mutex = threading.Lock()
    def callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        print(f"OK: {dp.data}")
        with counter_mutex:
            counter = counter + 1

    def exit_callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        # get last module in the pipeline
        print(f"EXIT: {dp.data}")

        with counter_mutex:
            counter = counter + 1

    def overflown_callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        print(f"OVERFLOWN: {dp.data}")
        with counter_mutex:
            counter = counter + 1
            
    def outdated_callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        print(f"OUTDATED: {dp.data}")
        with counter_mutex:
            counter = counter + 1

    def error_callback(dp: DataPackage[Data]) -> None:
        nonlocal counter, counter_mutex
        f_json = format_json(f"{dp.errors[0]}")
        print(f"ERROR: {f_json}")
        with counter_mutex:
            counter = counter + 1

    # Function to execute the processing pipeline
    def process_data(data: Data) -> Union[DataPackage, None]:
        return pipeline.execute(data, pip_ex_id, callback, exit_callback, overflown_callback, outdated_callback, error_callback)

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


    f_dp = format_json(f"{dp}")
    print(f"Example DataPackage: {f_dp}")
    time.sleep(100)
    print("THE END")
    api.stop()

    # time.sleep(1000)

if __name__ == "__main__":
    main()