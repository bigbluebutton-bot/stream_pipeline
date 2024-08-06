import json
import os
import sys
import threading
import traceback
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Dict, List, NamedTuple, Union
import uuid

from . import data_pb2
from .thread_safe_class import ThreadSafeClass


class RemoteException(Exception):
    def __init__(self, error: 'Error'):
        self.id = error.id
        self.type = error.type
        self.message = error.message
        self.traceback = error.traceback
        self.thread = error.thread
        self.start_context = error.start_context
        self.thread_id = error.thread_id
        self.is_daemon = error.is_daemon
        self.local_vars = error.local_vars
        self.global_vars = error.global_vars
        self.environment_vars = error.environment_vars
        self.module_versions = error.module_versions
        super().__init__(self.message)

    def to_error(self) -> 'Error':
        err = Error()
        err.id=self.id
        err.type=self.type
        err.message=self.message
        err.traceback=self.traceback
        err.thread=self.thread
        err.start_context=self.start_context
        err.thread_id=self.thread_id
        err.is_daemon=self.is_daemon
        err.local_vars=self.local_vars
        err.global_vars=self.global_vars
        err.environment_vars=self.environment_vars
        err.module_versions=self.module_versions
        return err


@dataclass
class Error (ThreadSafeClass, Exception):
    _id: str = field(default_factory=lambda: "Error-" + str(uuid.uuid4()))
    _type: str = ""
    _message: str = ""
    _traceback: List[str] = field(default_factory=list)
    _thread: Union[str, None] = None
    _start_context: Union[str, None] = None
    _thread_id: Union[int, None] = None
    _is_daemon: Union[bool, None] = None
    _local_vars: Dict[str, str] = field(default_factory=dict)
    _global_vars: Dict[str, str] = field(default_factory=dict)
    _environment_vars: Dict[str, str] = field(default_factory=dict)
    _module_versions: Dict[str, str] = field(default_factory=dict)
    
    _ThreadSafeClass__immutable_attributes: List[str] = field(default_factory=lambda: [])

    @property
    def id(self) -> str:
        return self._get_attribute('id')

    @id.setter
    def id(self, value: str) -> None:
        self._set_attribute('id', value)

    @property
    def type(self) -> str:
        return self._get_attribute('type')

    @type.setter
    def type(self, value: str) -> None:
        self._set_attribute('type', value)

    @property
    def message(self) -> str:
        return self._get_attribute('message')

    @message.setter
    def message(self, value: str) -> None:
        self._set_attribute('message', value)

    @property
    def traceback(self) -> List[str]:
        return self._get_attribute('traceback')

    @traceback.setter
    def traceback(self, value: List[str]) -> None:
        self._set_attribute('traceback', value)

    @property
    def thread(self) -> Union[str, None]:
        return self._get_attribute('thread')

    @thread.setter
    def thread(self, value: Union[str, None]) -> None:
        self._set_attribute('thread', value)

    @property
    def start_context(self) -> Union[str, None]:
        return self._get_attribute('start_context')

    @start_context.setter
    def start_context(self, value: Union[str, None]) -> None:
        self._set_attribute('start_context', value)

    @property
    def thread_id(self) -> Union[int, None]:
        return self._get_attribute('thread_id')

    @thread_id.setter
    def thread_id(self, value: Union[int, None]) -> None:
        self._set_attribute('thread_id', value)

    @property
    def is_daemon(self) -> Union[bool, None]:
        return self._get_attribute('is_daemon')

    @is_daemon.setter
    def is_daemon(self, value: Union[bool, None]) -> None:
        self._set_attribute('is_daemon', value)

    @property
    def local_vars(self) -> Dict[str, str]:
        return self._get_attribute('local_vars')

    @local_vars.setter
    def local_vars(self, value: Dict[str, str]) -> None:
        self._set_attribute('local_vars', value)

    @property
    def global_vars(self) -> Dict[str, str]:
        return self._get_attribute('global_vars')

    @global_vars.setter
    def global_vars(self, value: Dict[str, str]) -> None:
        self._set_attribute('global_vars', value)

    @property
    def environment_vars(self) -> Dict[str, str]:
        return self._get_attribute('environment_vars')

    @environment_vars.setter
    def environment_vars(self, value: Dict[str, str]) -> None:
        self._set_attribute('environment_vars', value)

    @property
    def module_versions(self) -> Dict[str, str]:
        return self._get_attribute('module_versions')

    @module_versions.setter
    def module_versions(self, value: Dict[str, str]) -> None:
        self._set_attribute('module_versions', value)

    def __str__(self) -> str:
        return json_error_handler_str(self)
    
    def to_remote_exception(self) -> RemoteException:
        return RemoteException(self)

    def set_from_grpc(self, grpc_error: data_pb2.Error) -> None:
        self.id = grpc_error.id
        self.type = grpc_error.type
        self.message = grpc_error.message
        self.traceback = list(grpc_error.traceback)  # Convert repeated fields to list
        self.thread = grpc_error.thread  # Directly set the field
        self.start_context = grpc_error.start_context  # Directly set the field
        self.thread_id = grpc_error.thread_id  # Directly set the field
        self.is_daemon = grpc_error.is_daemon  # Directly set the field
        self.local_vars = dict(grpc_error.local_vars)  # Convert map fields to dict
        self.global_vars = dict(grpc_error.global_vars)  # Convert map fields to dict
        self.environment_vars = dict(grpc_error.environment_vars)  # Convert map fields to dict
        self.module_versions = dict(grpc_error.module_versions)  # Convert map fields to dict

    def to_grpc(self) -> data_pb2.Error:
        grpc_error = data_pb2.Error()
        grpc_error.id = self.id
        grpc_error.type = self.type
        grpc_error.message = self.message
        grpc_error.traceback.extend(self.traceback)
        if self.thread is not None:
            grpc_error.thread = self.thread
        if self.start_context is not None:
            grpc_error.start_context = self.start_context
        if self.thread_id is not None:
            grpc_error.thread_id = self.thread_id
        if self.is_daemon is not None:
            grpc_error.is_daemon = self.is_daemon
        grpc_error.local_vars.update(self.local_vars)
        grpc_error.global_vars.update(self.global_vars)
        grpc_error.environment_vars.update(self.environment_vars)
        grpc_error.module_versions.update(self.module_versions)
        return grpc_error
    
    def to_exception(self) -> Exception:
        return RemoteException(self)

class ErrorLoggerOptions(NamedTuple):
    id: bool = True
    exc_type: bool = True
    message: bool = True
    traceback: bool = True
    thread: bool = True
    start_context: bool = True
    thread_id: bool = True
    is_daemon: bool = True
    local_vars: bool = False
    global_vars: bool = False
    environment_vars: bool = False
    module_versions: bool = False

class ErrorLogger:
    _instance = None # type: ignore
    _lock = threading.Lock()

    def __new__(cls) -> 'ErrorLogger':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ErrorLogger, cls).__new__(cls)
                    cls._instance.options = ErrorLoggerOptions()
                    cls._instance.debug = True
        return cls._instance

    def set_options(self, options: ErrorLoggerOptions) -> None:
        with self._lock:
            self.options = options

    def get_options(self) -> ErrorLoggerOptions:
        with self._lock:
            return self.options
    
    def set_debug(self, debug: bool) -> None:
        with self._lock:
            self.debug = debug
    
    def get_debug(self) -> bool:
        with self._lock:
            return self.debug

def format_vars(variables: Dict[str, Any]) -> Dict[str, str]:
    formatted_vars = {}
    for key, value in variables.items():
        formatted_vars[key] = f"{type(value).__name__} = {repr(value)}"
    return formatted_vars

def exception_to_error(exc: Union[BaseException, Error, None]) -> Union[Error, None]:
    if exc is None:
        return None
    if isinstance(exc, Error):
        return exc
    if isinstance(exc, RemoteException):
        return exc.to_error()
    exc_type = type(exc)
    exc_value = exc
    exc_traceback = exc.__traceback__
    
    tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    formatted_traceback: List[str] = []

    local_vars: Dict[str, str] = {}
    global_vars: Dict[str, str] = {}

    if exc_traceback is not None:
        tb = exc_traceback
        while tb.tb_next:
            tb = tb.tb_next
        frame = tb.tb_frame
        local_vars = format_vars(frame.f_locals)
        global_vars = format_vars({key: value for key, value in frame.f_globals.items() if not key.startswith('__')})

    for line in tb_lines:
        if line.startswith('  File '):
            parts = line.strip().split(',')
            file_path = parts[0].split('"')[1]
            line_number = parts[1].split(' ')[-1]
            formatted_traceback.append(f"{file_path}:{line_number}")
        else:
            formatted_traceback.append(line.strip())

    current_thread = threading.current_thread()
    start_context = getattr(current_thread, 'start_context', 'N/A')

    error = Error()
    error.type=exc_type.__name__
    error.message=str(exc_value)
    error.traceback=formatted_traceback
    error.thread=current_thread.name
    error.start_context=start_context
    error.thread_id=current_thread.ident
    error.is_daemon=current_thread.daemon
    error.local_vars=local_vars
    error.global_vars=global_vars
    error.environment_vars={key: os.environ[key] for key in os.environ}
    error.module_versions={module: sys.modules[module].__version__ if hasattr(sys.modules[module], '__version__') else 'N/A' for module in sys.modules}

    return error

def json_error_handler_dict(exc: Union[BaseException, Error, None]) -> Union[Dict[str, str], None]:
    if exc is None:
        return {}

    if isinstance(exc, BaseException):
        error_obj = exception_to_error(exc)
    else:
        error_obj = exc

    if error_obj is None:
        return None

    error_logger = ErrorLogger()
    options = error_logger.get_options()
    
    minimal_error_info = {
        "message": "Something went wrong.",
    }

    if error_logger.get_debug():
        if options.id:
            minimal_error_info["id"] = error_obj.id
        if options.exc_type:
            minimal_error_info["type"] = error_obj.type
        if options.message:
            minimal_error_info["message"] = error_obj.message
        if options.traceback:
            minimal_error_info["traceback"] = error_obj.traceback # type: ignore
        if options.thread:
            minimal_error_info["thread"] = error_obj.thread # type: ignore
        if options.start_context:
            minimal_error_info["start_context"] = error_obj.start_context # type: ignore
        if options.thread_id:
            minimal_error_info["thread_id"] = error_obj.thread_id # type: ignore
        if options.is_daemon:
            minimal_error_info["is_daemon"] = error_obj.is_daemon # type: ignore
        if options.local_vars:
            minimal_error_info["local_vars"] = error_obj.local_vars # type: ignore
        if options.global_vars:
            minimal_error_info["global_vars"] = error_obj.global_vars # type: ignore
        if options.environment_vars:
            minimal_error_info["environment_vars"] = error_obj.environment_vars # type: ignore
        if options.module_versions:
            minimal_error_info["module_versions"] = error_obj.module_versions # type: ignore

    return minimal_error_info

def json_error_handler_str(exc: Union[BaseException, Error, None]) -> str:
    minimal_error_info = json_error_handler_dict(exc) 
    error_json = json.dumps(minimal_error_info, indent=4)
    return error_json

# Custom JSON error handler
def json_error_handler(exc: Union[BaseException, Error, None]) -> None:
    error_json = json_error_handler_str(exc)
    print(error_json)

# Set the custom error handler for uncaught exceptions
def custom_excepthook(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: TracebackType | None) -> None:
    json_error_handler(exc_value)

sys.excepthook = custom_excepthook

# Set the custom error handler for uncaught exceptions in threads
def threading_excepthook(args: threading.ExceptHookArgs) -> None:
    json_error_handler(args.exc_value)

threading.excepthook = threading_excepthook

# Example usage
if __name__ == "__main__":
    error_logger = ErrorLogger()
    error_logger.set_debug(True)  # Enable or disable debug logging

    # Set custom options
    custom_options = ErrorLoggerOptions(
        id = True,
        exc_type = True,
        message = True,
        traceback = True,
        thread = True,
        start_context = True,
        thread_id = True,
        is_daemon = False,
        local_vars = True,
        global_vars = False,
        environment_vars = False,
        module_versions = False,
    )
    error_logger.set_options(custom_options)

    def faulty_function() -> None:
        x = 42
        y = 'example'
        raise ValueError("An example error for testing")
    
    def execute_function() -> None:
        faulty_function()

    def thread_level_3() -> None:
        execute_function()

    def thread_level_2() -> None:
        thread3 = threading.Thread(target=thread_level_3, name="Thread-Level-3")
        thread3.start_context = threading.current_thread().name # type: ignore
        thread3.start()
        thread3.join()

    def thread_level_1() -> None:
        thread2 = threading.Thread(target=thread_level_2, name="Thread-Level-2")
        thread2.start_context = threading.current_thread().name # type: ignore
        thread2.start()
        thread2.join()

    # Start the top-level thread
    print(f"-------------------In Thread-------------------")
    thread1 = threading.Thread(target=thread_level_1, name="Thread-Level-1")
    thread1.start_context = threading.current_thread().name # type: ignore
    thread1.start()
    thread1.join()
    print("-----------------------------------------------\n")

    # Example usage in the main thread to also demonstrate main thread error handling
    print(f"---------In MainThread with try except---------")
    try:
        execute_function()
    except Exception as e:
        json_error_handler(e)
    print(f"-----------------------------------------------\n")
        
    print(f"-----------In MainThread with crash------------")
    execute_function()
    print(f"-----------------------------------------------\n")
