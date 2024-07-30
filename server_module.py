import grpc # type: ignore
from concurrent import futures
import time
from typing import Any, Tuple, Union

from src.data_package import DataPackage, DataPackageModule
from src.error import Error, exception_to_error

from src import data_pb2  # Correct import statement
from src import data_pb2_grpc  # Correct import statement

def grpc_to_normal(request_grpc: Any) -> Tuple[DataPackage, Union[None, DataPackageModule]]:
    dp = DataPackage()
    dp.set_from_grpc(request_grpc.data_package)
    dpm = DataPackageModule()
    dpm.set_from_grpc(request_grpc.data_package_module)

    # find the dpm in dp.modules and set it to dpm so they are the same object
    for module in dp.modules:
        if module.module_id == dpm.module_id:
            dpm = module
            break

    return dp, dpm

def normal_to_grpc(request: DataPackage, error: Union[None, Exception, Error] = None) -> Any:
    error_grpc: Union[None, Any] = None
    if error:
        error = exception_to_error(error)
        if error:
            error_grpc = error.to_grpc()
    request_grpc = request.to_grpc()

    return_dp_and_error = data_pb2.ReturnDPandError()  # type: ignore
    return_dp_and_error.data_package.CopyFrom(request_grpc)
    if error_grpc:
        return_dp_and_error.error.CopyFrom(error_grpc)
    return return_dp_and_error

class ModuleServiceServicer(data_pb2_grpc.ModuleServiceServicer):
    def run(self, request_grpc: data_pb2.RequestDPandDPM, context: grpc.ServicerContext) -> data_pb2.ReturnDPandError:  # type: ignore
        testerror: Union[None, Exception, Error] = None
        try:
            data_package, data_package_module = grpc_to_normal(request_grpc)

            data_package.message = "This is a test message"
            print(data_package_module)

            raise ValueError("This is a test error")
        except Exception as e:
            testerror = e

        try:
            return_dp_and_error = normal_to_grpc(data_package, testerror)
            return return_dp_and_error
        except Exception as e:
            print(exception_to_error(e))

        return data_pb2.ReturnDPandError()  # type: ignore # Return empty object

def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_pb2_grpc.add_ModuleServiceServicer_to_server(ModuleServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
