from concurrent import futures
import grpc # type: ignore
import time
from typing import Any, Generic, Optional, Tuple, TypeVar, Union

from .data_package import DataPackage, DataPackagePhaseController, DataPackagePhase, DataPackageModule

from .error import exception_to_error

from .module_classes import Module 
from .data_pb2 import ReturnDPandError, RequestDP
from .data_pb2_grpc import ModuleServiceServicer as ModuleServiceServicerBase, add_ModuleServiceServicer_to_server

T = TypeVar('T')

# ModuleServiceServicer implementation
class ModuleServiceServicer(Generic[T], ModuleServiceServicerBase):
    def __init__(self, module: Module):
        self.module = module

    def run(self, request_grpc: RequestDP, context: grpc.ServicerContext) -> ReturnDPandError:
        dp: Union[None, DataPackage] = None
        try:
            # Convert gRPC request to normal objects
            dp, dpc, dpp, dpm = self.grpc_to_normal(request_grpc)
            
            # Run the module with data_package and data_package_module
            self.module.run(dp, dpc, dpp, dpm)
            
            # Convert the result back to gRPC response
            return_dp_and_error = self.normal_to_grpc(dp)
            return return_dp_and_error
        except Exception as e:
            # In case of any exception, convert it to a gRPC response
            try:
                return_dp_and_error = self.normal_to_grpc(dp, e)
                return return_dp_and_error
            except Exception as nested_exception:
                return_dp_and_error = ReturnDPandError()
                return_dp_and_error.error.CopyFrom(exception_to_error(nested_exception).to_grpc()) # type: ignore
                return return_dp_and_error
            
    # Function to convert gRPC message to normal objects
    def grpc_to_normal(self, request_grpc: RequestDP) -> Tuple[DataPackage[T], DataPackagePhaseController, DataPackagePhase, DataPackageModule]:
        dp = DataPackage[T]()
        dp.set_from_grpc(request_grpc.data_package)
        dpc_id = request_grpc.data_package_controller_id
        dpp_id = request_grpc.data_package_phase_id
        dpm_id = request_grpc.data_package_module_id

        def find_controller(dp: DataPackage, dpm_id: str) -> Optional[DataPackagePhaseController]:
            for dpc in dp.controller:
                if dpc.id == dpc_id:
                    return dpc
            return None
        
        def find_phase(dpc: DataPackagePhaseController, dpp_id: str) -> Optional[DataPackagePhase]:
            for dpp in dpc.phases:
                if dpp.id == dpp_id:
                    return dpp
            return None
        
        def find_module(dpp: Union[DataPackagePhase, DataPackageModule], dpm_id: str) -> Optional[DataPackageModule]:
            modules = []
            if isinstance(dpp, DataPackagePhase):
                modules = dpp.modules
            elif isinstance(dpp, DataPackageModule):
                modules = dpp.sub_modules
            else:
                raise ValueError(f"Encoder gRPC to DataPackage: Phase not found. Type not recognized: {type(dpp)}")

            for dpm in modules:
                if dpm.id == dpm_id:
                    return dpm
                elif dpm.sub_modules:
                    for sub_dpm in dpm.sub_modules:
                        found_dpm = find_module(sub_dpm, dpm_id)
                        if found_dpm:
                            return found_dpm
            return None

        dpc = find_controller(dp, dpc_id)
        if not dpc:
            raise ValueError("Encoder gRPC to DataPackage: Controller not found")
        dpp = find_phase(dpc, dpp_id)
        if not dpp:
            raise ValueError("Encoder gRPC to DataPackage: Phase not found")
        dpm = find_module(dpp, dpm_id)
        if not dpm:
            raise ValueError("Encoder gRPC to DataPackage: Module not found")

        return dp, dpc, dpp, dpm

    # Function to convert normal objects to gRPC messages
    def normal_to_grpc(self, request: Union[DataPackage[T], None], error: Union[None, Exception] = None) -> ReturnDPandError:
        error_grpc: Union[None, Any] = None
        if error:
            error = exception_to_error(error)
            if error:
                error_grpc = error.to_grpc()
        

        request_grpc = request.to_grpc() if request else None

        return_dp_and_error = ReturnDPandError()
        if request_grpc:
            return_dp_and_error.data_package.CopyFrom(request_grpc)
        if error_grpc:
            return_dp_and_error.error.CopyFrom(error_grpc)
        return return_dp_and_error

# gRPC server class
class GrpcServer(Generic[T]):
    def __init__(self, module: Module, port: int):
        self.module = module
        self.port = port
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_ModuleServiceServicer_to_server(ModuleServiceServicer[T](self.module), self.server)
        self.server.add_insecure_port(f'[::]:{self.port}')
    
    def start(self) -> None:
        self.server.start()
        print(f"Server started on port {self.port}.")
    
    def stop(self) -> None:
        self.server.stop(0)
        print(f"Server on port {self.port} stopped.")

    def wait_for_termination(self) -> None:
        try:
            while True:
                time.sleep(86400)
        except KeyboardInterrupt:
            self.stop()
