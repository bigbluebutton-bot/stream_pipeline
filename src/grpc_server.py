from concurrent import futures
import grpc # type: ignore
import time
from typing import Any, Tuple, Union

from .error import exception_to_error

from .module_classes import Module, DataPackage, DataPackageModule
from .data_pb2 import ReturnDPandError, RequestDPandDPM # type: ignore
from .data_pb2_grpc import ModuleServiceServicer as ModuleServiceServicerBase, add_ModuleServiceServicer_to_server # type: ignore

# Function to convert gRPC message to normal objects
def grpc_to_normal(request_grpc: RequestDPandDPM) -> Tuple[DataPackage, Union[None, DataPackageModule]]:
    dp = DataPackage()
    dp.set_from_grpc(request_grpc.data_package)
    dpm = DataPackageModule()
    dpm.set_from_grpc(request_grpc.data_package_module)

    def find_module(data_package_module: DataPackageModule) -> Union[None, DataPackageModule]:
        for sm in data_package_module.sub_modules:
            if sm.module_id == dpm.module_id:
                return sm
            found_module = find_module(sm)
            if found_module:
                return found_module
        return None

    def find_and_set_module(dp: DataPackage, dpm: DataPackageModule) -> DataPackageModule:
        for phase in dp.phases:
            for module in phase.modules:
                if module.module_id == dpm.module_id:
                    return module
                found_module = find_module(module)
                if found_module:
                    return found_module
        return dpm

    dpm = find_and_set_module(dp, dpm)
    return dp, dpm

# Function to convert normal objects to gRPC messages
def normal_to_grpc(request: DataPackage, error: Union[None, Exception] = None) -> ReturnDPandError:
    error_grpc: Union[None, Any] = None
    if error:
        error = exception_to_error(error)
        if error:
            error_grpc = error.to_grpc()
    request_grpc = request.to_grpc()

    return_dp_and_error = ReturnDPandError()
    return_dp_and_error.data_package.CopyFrom(request_grpc)
    if error_grpc:
        return_dp_and_error.error.CopyFrom(error_grpc)
    return return_dp_and_error

# ModuleServiceServicer implementation
class ModuleServiceServicer(ModuleServiceServicerBase):
    def __init__(self, module: Module):
        self.module = module

    def run(self, request_grpc: RequestDPandDPM, context: grpc.ServicerContext) -> ReturnDPandError:
        try:
            # Convert gRPC request to normal objects
            data_package, data_package_module = grpc_to_normal(request_grpc)
            
            # Run the module with data_package and data_package_module
            self.module.run(data_package, data_package_module)
            
            # Convert the result back to gRPC response
            return_dp_and_error = normal_to_grpc(data_package)
            return return_dp_and_error
        except Exception as e:
            # In case of any exception, convert it to a gRPC response
            try:
                return_dp_and_error = normal_to_grpc(data_package, e)
                return return_dp_and_error
            except:
                return_dp_and_error = ReturnDPandError()
                return_dp_and_error.error.CopyFrom(exception_to_error(e).to_grpc()) # type: ignore
                return return_dp_and_error

# gRPC server class
class GrpcServer:
    def __init__(self, module: Module, port: int):
        self.module = module
        self.port = port
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_ModuleServiceServicer_to_server(ModuleServiceServicer(self.module), self.server)
        self.server.add_insecure_port(f'[::]:{self.port}')
    
    def start(self):
        self.server.start()
        print(f"Server started on port {self.port}.")
    
    def stop(self):
        self.server.stop(0)
        print(f"Server on port {self.port} stopped.")

    def wait_for_termination(self):
        try:
            while True:
                time.sleep(86400)
        except KeyboardInterrupt:
            self.stop()
