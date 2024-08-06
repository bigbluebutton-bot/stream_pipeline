def main() -> None:
    import random
    import time
    from typing import Union
    from stream_pipeline.grpc_server import GrpcServer
    from stream_pipeline.data_package import DataPackage, DataPackageModule
    from stream_pipeline.module_classes import ExecutionModule, ModuleOptions

    # Creating a data type
    class Data:
        def __init__(self, key: str, condition: bool) -> None:
            self.key = key
            self.condition = condition
            self.status = "unknown"
            
        def __str__(self) -> str:
            return f"Data: {self.key}, {self.condition}"


    class TestModule(ExecutionModule):
        def __init__(self) -> None:
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=4.0
            ))

        def execute(self, dp: DataPackage[Data], dpm: DataPackageModule) -> None:
            list1 = [1, 2, 3]
            randomint = random.choice(list1)
            time.sleep(randomint)
            if dp.data:
                dp.data.key = dp.data.key.upper() + " transformed"
                dpm.success = True
                dpm.message = "Transformation succeeded"
            else:
                dpm.success = False
                dpm.message = "Transformation failed: key missing"

    # Example usage
    module = TestModule()
    server = GrpcServer[Data](module, 50051)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    main()