from dataclasses import dataclass, field, asdict
from typing import Any, Dict
import threading
import uuid
import copy

@dataclass
class DataPackage:
    """
    Class which contains the data and metadata for a pipeline process and will be passed through the pipeline and between modules.
    Attributes:
        id (str) immutable:                     Unique identifier for the data package.
        pipeline_executer_id (str) immutable:   ID of the pipeline executor handling this package.
        sequence_number (int):                  The sequence number of the data package.
        data (Any):                             The actual data contained in the package.
        success (bool):                         Indicates if the process was successful.
        message (str):                          Message related to the process.
    """
    id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    pipeline_executer_id: str
    sequence_number: int
    data: Any = None
    success: bool = False
    message: str = ""

    # Immutable attributes
    _immutable_attributes: list = field(default_factory=lambda: 
                                            [
                                                'id',
                                                'pipeline_executer_id',
                                            ]
                                        )

    # Mutexes for thread-safe property access
    _mutexes: Dict[str, threading.Lock] = field(default_factory=dict, init=False)

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

# Example usage code
def main():
    # Create an instance of DataPackage
    package = DataPackage(
        pipeline_executer_id="executor_1",
        sequence_number=1,
        data={"key": "value"},
        success=True,
        message="Process completed successfully."
    )

    # Access properties
    print(f"ID: {package.id}")
    print(f"Pipeline Executor ID: {package.pipeline_executer_id}")
    print(f"Sequence Number: {package.sequence_number}")
    print(f"Data: {package.data}")
    print(f"Success: {package.success}")
    print(f"Message: {package.message}")

    # Attempt to modify the immutable id property (will raise an exception)
    try:
        package.id = "67890"
    except AttributeError as e:
        print(e)

    # Modify other properties
    package.sequence_number = 2
    package.data = {"new_key": "new_value"}
    package.success = False
    package.message = "Process failed."

    # Access modified properties
    print(f"Modified Pipeline Executor ID: {package.pipeline_executer_id}")
    print(f"Modified Sequence Number: {package.sequence_number}")
    print(f"Modified Data: {package.data}")
    print(f"Modified Success: {package.success}")
    print(f"Modified Message: {package.message}")

    # Adding new property
    package.new_property = "This is a new property"
    print(f"New Property: {package.new_property}")

    print("------------------")
    package2 = package.copy()
    package.sequence_number = 3 # Modify the original package to ensure deep copy
    print(f"ID: {package2.id}")
    print(f"Pipeline Executor ID: {package2.pipeline_executer_id}")
    print(f"Sequence Number: {package2.sequence_number}")
    print(f"Data: {package2.data}")
    print(f"Success: {package2.success}")
    print(f"Message: {package2.message}")

    # Ensure the new property is also copied
    print(f"Copied New Property: {package2.new_property}")

if __name__ == "__main__":
    main()
