

from typing import Any, Callable, Dict, List, Optional

import uvicorn
import threading
from fastapi import FastAPI
from .pipeline import Pipeline

def add_route(
    sub_path: str, 
    methods: List[str], 
    summary: Optional[str] = None, 
    description: Optional[str] = None
) -> Callable[[Callable[..., Dict[str, Any]]], Callable[..., Dict[str, Any]]]:
    # Return a decorator that will wrap the method
    def decorator(func: Callable[..., Dict[str, Any]]) -> Callable[..., Dict[str, Any]]:
        # Store the route information in the function's __dict__
        func.__dict__['__annotations__'] = {
            'add_route': {
                'sub_path': sub_path,
                'methods': methods,
                'summary': summary,
                'description': description
            }
        }
        return func
    return decorator



class APIService:
    def __init__(self, host: str, port: int, pipeline: Pipeline):
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.version: str = "v1"
        self._app = FastAPI()
        self._server_thread = None
        self._uvicorn_server = None


    def _load_routes(self):
        # self.pipeline._instances_controllers[ex_id][0]._phases[0]._modules[0]
        
        pipeline_id = self.pipeline.get_id()
        instances_id = self.pipeline.get_instances()
        for instance_id in instances_id:
            controllers = self.pipeline._instances_controllers[instance_id]
            for controller in controllers:
                phases = controller._phases
                for phase in phases:
                    modules = phase._modules
                    for module in modules:
                        routes = module._get_api_annotation()
                        for method, route in routes.items():
                            module_id = module.get_id()
                            root_path = f"/api/{self.version}/pipelines/{pipeline_id}/instances/{instance_id}/modules/{module_id}"
                            api_path = f"{root_path}{route['sub_path']}"
                            self._app.add_api_route(
                                api_path,
                                endpoint=method,
                                methods=route['methods'],
                                summary=route['summary'],
                                description=route['description']
                            )
        
    def run(self):
        self._load_routes()
        """Start FastAPI server in a separate thread."""
        if self._server_thread is None or not self._server_thread.is_alive():
            config = uvicorn.Config(self._app, host=self.host, port=self.port, log_level="info")
            self._uvicorn_server = uvicorn.Server(config)
            
            self._server_thread = threading.Thread(target=self._uvicorn_server.run)
            self._server_thread.daemon = True  # Ensures thread stops when the main program exits
            self._server_thread.start()
            print("FastAPI started.")

    def stop(self):
        """Gracefully stop FastAPI without killing the program."""
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True  # Signal Uvicorn to stop
            print("FastAPI stopping...")