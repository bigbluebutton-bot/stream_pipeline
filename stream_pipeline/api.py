from typing import Any, Callable, Dict, List, Optional
from pydantic import BaseModel
import uvicorn
import threading
from fastapi import FastAPI
from .pipeline import Pipeline

def add_route(
    sub_path: str,
    methods: List[str],
    summary: Optional[str] = None,
    description: Optional[str] = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that stores route information in the function's __dict__,
    so it can be picked up later for FastAPI route creation.
    """
    def decorator(func: Callable[..., Dict[str, Any]]) -> Callable[..., Any]:
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

def _get_api_annotation(cls: Any) -> Dict[Callable[..., Dict[str, Any]], Dict[str, Any]]:
    result: Dict[Callable[..., Dict[str, Any]], Dict[str, Any]] = {}

    for method_name in dir(cls):
        method = getattr(cls, method_name)
        if hasattr(method, '__dict__') and 'add_route' in method.__dict__.get('__annotations__', {}):
            result[method] = method.__dict__['__annotations__']['add_route']

    return result

class APIService:
    def __init__(self, host: str, port: int, pipeline: Optional[Pipeline] = None):
        """
        Initialize the API service. Optionally accept a single Pipeline,
        but the service can handle multiple pipelines added later.
        """
        self.host: str = host
        self.port: int = port

        # FastAPI app
        self._app: FastAPI = FastAPI()

        # Dict to store pipelines keyed by pipeline_id
        self._pipelines: Dict[str, Pipeline] = {}
        # Map each pipeline_id to the list of route objects added to FastAPI
        self._routes_map: Dict[str, List] = {}
        
        self.version: str = "v1"
        self._server_thread: Optional[threading.Thread] = None
        self._uvicorn_server: Optional[uvicorn.Server] = None

        self._load_routes()

        # If a pipeline was passed in, add it immediately
        if pipeline is not None:
            self.add_pipeline(pipeline)


    def _load_routes(self) -> None:
        """
        Load api default routes
        """
        routes = _get_api_annotation(self)
        for endpoint_func, route_info in routes.items():
            api_path = f"/api/{self.version}{route_info['sub_path']}"
            self._app.router.add_api_route(
                path=api_path,
                endpoint=endpoint_func,
                methods=route_info['methods'],
                summary=route_info['summary'],
                description=route_info['description']
            )

    def _update_docs(self) -> None:
        # Update the OpenAPI schema
        self._app.openapi_schema = None  # Clear cached schema
        self._app.openapi()  # Recalculate schema

    def _add_routes_for_pipeline(self, pipeline_id: str) -> None:
        """
        Extracts routes from a pipeline (its controllers/modules) and adds them 
        to the FastAPI application. Also keeps track of those routes in _routes_map
        so they can be removed if the pipeline is later removed.
        """
        pipeline = self._pipelines[pipeline_id]
        # Prepare a container for the pipeline's routes
        route_objects = []

        instances_id = pipeline.get_instances()
        for instance_id in instances_id:
            controllers = pipeline._instances_controllers[instance_id]
            for controller in controllers:
                phases = controller._phases
                for phase in phases:
                    modules = phase._modules
                    for module in modules:
                        routes = _get_api_annotation(module)
                        for endpoint_func, route_info in routes.items():
                            module_id = module.get_id()
                            root_path = f"/api/{self.version}/pipelines/{pipeline_id}/instances/{instance_id}/modules/{module_id}"
                            api_path = f"{root_path}{route_info['sub_path']}"
                            
                            # Add route to the FastAPI app
                            self._app.router.add_api_route(
                                path=api_path,
                                endpoint=endpoint_func,
                                methods=route_info['methods'],
                                summary=route_info['summary'],
                                description=route_info['description']
                            )
                            # get route object
                            new_route = next((route for route in self._app.routes if route.path == api_path), None) # type: ignore

                            if new_route is not None:
                                route_objects.append(new_route)
        
        self._routes_map[pipeline_id] = route_objects
        self._update_docs()

    def _remove_routes_for_pipeline(self, pipeline_id: str) -> None:
        """
        Remove all routes associated with a pipeline from the FastAPI app.
        """
        pipeline_routes = self._routes_map.pop(pipeline_id, [])
        for r in pipeline_routes:
            if r in self._app.router.routes:
                self._app.router.routes.remove(r)
        self._update_docs()

    def _add_routes_for_instance(self, pipeline_id: str, instance_id: str) -> None:
        """
        Add all routes associated with a pipeline instance to the FastAPI app.
        """
        pipeline = self._pipelines[pipeline_id]
        route_objects = []

        controllers = pipeline._instances_controllers[instance_id]
        for controller in controllers:
            phases = controller._phases
            for phase in phases:
                modules = phase._modules
                for module in modules:
                    routes = _get_api_annotation(module)
                    for endpoint_func, route_info in routes.items():
                        module_id = module.get_id()
                        root_path = f"/api/{self.version}/pipelines/{pipeline_id}/instances/{instance_id}/modules/{module_id}"
                        api_path = f"{root_path}{route_info['sub_path']}"
                        
                        # Add route to the FastAPI app
                        self._app.router.add_api_route(
                            path=api_path,
                            endpoint=endpoint_func,
                            methods=route_info['methods'],
                            summary=route_info['summary'],
                            description=route_info['description']
                        )
                        # get route object
                        new_route = next((route for route in self._app.routes if route.path == api_path), None) # type: ignore

                        if new_route is not None:
                            route_objects.append(new_route)

        self._routes_map[pipeline_id] += route_objects
        self._update_docs()

    def _remove_routes_for_instance(self, pipeline_id: str, instance_id: str) -> None:
        """
        Remove all routes associated with a pipeline instance from the FastAPI app.
        """
        pipeline_routes = self._routes_map.get(pipeline_id, [])
        instance_routes = [r for r in pipeline_routes if f"/instances/{instance_id}/" in r.path]
        for r in instance_routes:
            if r in self._app.router.routes:
                self._app.router.routes.remove(r)
        self._update_docs()

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """
        Add a new pipeline to the service and create FastAPI routes for it.
        """
        pipeline_id = pipeline.get_id()
        if pipeline_id in self._pipelines:
            # Already have this pipeline; optionally raise an error or just skip
            print(f"Pipeline {pipeline_id} is already registered.")
            return
        
        self._pipelines[pipeline_id] = pipeline
        # Load routes for just this pipeline
        self._add_routes_for_pipeline(pipeline_id)

        pipeline._set_api_callbacks(
            self._add_routes_for_pipeline,
            self._add_routes_for_instance,
            self._remove_routes_for_pipeline,
            self._remove_routes_for_instance
        )

        print(f"Pipeline {pipeline_id} added. Routes registered.")

    def remove_pipeline(self, pipeline_id: str) -> None:
        """
        Remove a pipeline from the service and also remove its FastAPI routes.
        """
        if pipeline_id not in self._pipelines:
            print(f"Pipeline {pipeline_id} not found.")
            return
        
        # 1. Remove the pipeline from our dictionary
        del self._pipelines[pipeline_id]
        
        # 2. Remove any routes that were created for this pipeline
        self._remove_routes_for_pipeline(pipeline_id)

        print(f"Pipeline {pipeline_id} removed. Routes unregistered.")

    def run(self) -> None:
        """Start FastAPI server in a separate thread."""
        if self._server_thread is None or not self._server_thread.is_alive():
            config = uvicorn.Config(self._app, host=self.host, port=self.port, log_level="info")
            self._uvicorn_server = uvicorn.Server(config)
            
            self._server_thread = threading.Thread(target=self._uvicorn_server.run)
            self._server_thread.daemon = True
            self._server_thread.start()
            print("FastAPI started.")

    def stop(self) -> None:
        """Gracefully stop FastAPI without killing the program."""
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
            print("FastAPI stopping...")


    class Response_get_pipelines(BaseModel):
        pipelines: List[str]
    @add_route(
        sub_path="/pipelines",
        methods=["GET"],
        summary="Get all pipelines",
        description="Return all pipelines currently registered with the service."
    )
    def get_pipelines(self) -> Response_get_pipelines:
        """"
        Get the ids of all pipelines currently registered with the service.
        """
        return self.Response_get_pipelines(pipelines=list(self._pipelines.keys()))
    

    class Response_get_pipeline_instances(BaseModel):
        instances: List[str]
    @add_route(
        sub_path="/pipelines/{pipeline_id}/instances",
        methods=["GET"],
        summary="Get all instances of a pipeline",
        description="Return all instances of a pipeline."
    )
    def get_pipeline_instances(self, pipeline_id: str) -> Response_get_pipeline_instances:
        """"
        Get the ids of all instances of a pipeline.
        """
        if pipeline_id not in self._pipelines:
            raise ValueError(f"Pipeline {pipeline_id} not found.")
        return self.Response_get_pipeline_instances(instances=list(self._pipelines[pipeline_id].get_instances()))