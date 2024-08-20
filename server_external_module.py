import json


def main() -> None:
    import random
    import time
    from prometheus_client import start_http_server
    from stream_pipeline.grpc_server import GrpcServer
    from stream_pipeline.data_package import DataPackage, DataPackageModule, DataPackagePhase, DataPackageController, Status
    from stream_pipeline.module_classes import ExecutionModule, ModuleOptions
    import stream_pipeline.error as error

    from data import Data

    def format_json(json_str: str) -> str:
        try:
            return json.dumps(json.loads(json_str), indent=4)
        except Exception as ex:
            return json_str

    err_logger = error.ErrorLogger()
    err_logger.set_debug(True)
    err_logger.set_custom_excepthook(lambda ex: print(f"{format_json(ex)}"))
    err_logger.set_custom_threading_excepthook(lambda ex: print(f"{format_json(ex)}"))

    start_http_server(8001)

    class TestModule(ExecutionModule):
        def __init__(self) -> None:
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=4.0
            ))

        def execute(self, dp: DataPackage[Data], dpc: DataPackageController, dpp: DataPackagePhase, dpm: DataPackageModule) -> None:
            list1 = [1, 2, 3]
            randomint = random.choice(list1)
            time.sleep(randomint)
            if dp.data:
                dp.data.key = dp.data.key.upper() + " transformed"
                dpm.message = "Transformation succeeded"
            else:
                dpm.status = Status.EXIT
                dpm.message = "Transformation failed: key missing"

    # Example usage
    module = TestModule()
    server = GrpcServer[Data](module, 50051)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    main()