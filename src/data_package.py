from dataclasses import dataclass, field, asdict
import json
from typing import Any, Dict, List, Optional, Tuple
import threading
import uuid
import copy
import time

from .error import json_error_handler_dict

@dataclass
class DataPackageModule:
    module_id: str
    success: bool
    error: Optional[Exception]
    start_time: float
    end_time: float
    waiting_time: float
    processing_time: float
    total_time: float

@dataclass
class DataPackage:
    """
    Class which contains the data and metadata for a pipeline process and will be passed through the pipeline and between modules.
    Attributes:
        id (str) immutable:                     Unique identifier for the data package.
        pipeline_id (str):                      ID of the pipeline handling this package.
        pipeline_executer_id (str) immutable:   ID of the pipeline executor handling this package.
        sequence_number (int):                  The sequence number of the data package.
        modules (List[DataPackageModule]):      List of modules that have processed the data package. Including measurements.
        data (Any):                             The actual data contained in the package.
        success (bool):                         Indicates if the process was successful. If not successful, the error attribute should be set.
        message (str):                          Info message.
        error (Exception):                      Error message if the process was not successful.
    """
    id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    pipeline_id: str
    pipeline_executer_id: str
    sequence_number: int
    modules: List[DataPackageModule] = field(default_factory=list)
    data: Any = None
    running: bool = False
    success: bool = True
    message: str = ""
    error: Exception = None

    # Immutable attributes
    _immutable_attributes: List[str] = field(default_factory=lambda: 
                                            [
                                                'id',
                                                'pipeline_id',
                                                'pipeline_executer_id',
                                            ]
                                        )

    # Mutexes for thread-safe property access
    _mutexes: dict = field(default_factory=dict, init=False)

    def __getattr__(self, name: str) -> Any:
        if '_mutexes' in self.__dict__:
            if name not in self._mutexes:
                self._mutexes[name] = threading.Lock()

            with self._mutexes[name]:
                return super().__getattribute__(name)
        else:
            return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any):
        if '_immutable_attributes' in self.__dict__:
            for attr in self._immutable_attributes:
                if name == attr and attr in self.__dict__:
                    raise AttributeError(f"'{self.__class__.__name__}' object attribute '{attr}' is immutable")

        if '_mutexes' in self.__dict__:
            if name not in self._mutexes:
                self._mutexes[name] = threading.Lock()
            with self._mutexes[name]:
                current_thread = threading.current_thread()
                if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                    raise RuntimeError("Modification not allowed: the thread handling this DataPackage has timed out.")
                super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def __deepcopy__(self, memo):
        # Create a deep copy of the object without the _mutexes
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in self.__dict__.items():
            if k != '_mutexes':
                setattr(result, k, copy.deepcopy(v, memo))

        # Reinitialize the _mutexes after copying
        result._mutexes = {k: threading.Lock() for k in self.__dict__.keys() if k != '_mutexes'}

        return result

    def copy(self):
        # Create a deep copy using the custom __deepcopy__ method
        return copy.deepcopy(self)

    def __str__(self):
        data_dict = {key: value for key, value in self.__dict__.items() if not key.startswith('_')}
        data_dict['error'] = json_error_handler_dict(self.error) if self.error else "N/A"
        data_dict['modules'] = [asdict(module) for module in data_dict['modules']]

        # Handle non-serializable data
        try:
            jsonstring = json.dumps(data_dict, default=str, indent=4)
        except (TypeError, ValueError):
            data_dict['data'] = "Data which cannot be displayed as JSON"
            jsonstring = json.dumps(data_dict, default=str, indent=4)

        return jsonstring


# Example usage code
def worker(data_package):
    try:
        # Simulate work
        time.sleep(5)
        # Modify the data package
        data_package.data = {"updated_key": "updated_value"}
        print("DataPackage modified by worker thread.")
    except RuntimeError as e:
        print(e)

def main():
    data_package = DataPackage(
        pipeline_id="pipeline_1",
        pipeline_executer_id="executor_1",
        sequence_number=1,
        data=b'\x00\x01'  # Non-serializable data (bytes)
    )

    print(data_package)
    
    # Create an instance of DataPackage
    package = DataPackage(
        pipeline_id="pipeline_1",
        pipeline_executer_id="executor_1",
        sequence_number=1,
        data={"key": "value"},
        success=True,
        error="No error"
    )

    # Start the worker thread
    worker_thread = threading.Thread(target=worker, args=(package,))
    worker_thread.timed_out = False
    worker_thread.start()
    worker_thread.join(timeout=1)  # Set timeout for the worker thread

    # Check if the worker thread is still alive
    if worker_thread.is_alive():
        print("Worker thread timed out.")
        worker_thread.timed_out = True

    # Access properties
    print(f"ID: {package.id}")
    print(f"Pipeline Executor ID: {package.pipeline_executer_id}")
    print(f"Sequence Number: {package.sequence_number}")
    print(f"Data: {package.data}")
    print(f"Success: {package.success}")
    print(f"Error: {package.error}")

    # Attempt to modify the package after timeout
    try:
        package.sequence_number = 2
    except RuntimeError as e:
        print(f"Error in main thread: {e}")

if __name__ == "__main__":
    main()
