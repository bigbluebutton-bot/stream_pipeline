import json
import os
import sys
import threading
import traceback
from types import TracebackType
from typing import Any, Dict, List, NamedTuple, Optional

class ErrorLoggerOptions(NamedTuple):
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
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ErrorLogger, cls).__new__(cls)
                    cls._instance.options = ErrorLoggerOptions()
                    cls._instance.debug = False
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

    
def json_error_handler_dict(exc: BaseException) -> dict[str, Any]:
    error_logger = ErrorLogger()
    exc_type = type(exc)
    exc_value = exc
    exc_traceback = exc.__traceback__
    
    tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    formatted_traceback: List[str] = []
    local_vars: Dict[str, Any] = {}
    global_vars: Dict[str, Any] = {}

    if exc_traceback is not None:
        tb = exc_traceback
        while tb.tb_next:
            tb = tb.tb_next
        frame = tb.tb_frame
        local_vars = {key: repr(value) for key, value in frame.f_locals.items()}
        global_vars = {key: repr(value) for key, value in frame.f_globals.items() if not key.startswith('__')}

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
    
    error_details: Dict[str, Any] = {}
    options = error_logger.get_options()

    if error_logger.get_debug():
        if options.exc_type:
            error_details["type"] = exc_type.__name__
        if options.message:
            error_details["message"] = str(exc_value)
        if options.traceback:
            error_details["traceback"] = formatted_traceback
        if options.thread:
            error_details["thread"] = current_thread.name
        if options.start_context:
            error_details["start_context"] = start_context
        if options.thread_id:
            error_details["thread_id"] = current_thread.ident
        if options.is_daemon:
            error_details["is_daemon"] = current_thread.daemon
        if options.local_vars:
            error_details["local_vars"] = local_vars
        if options.global_vars:
            error_details["global_vars"] = global_vars
        if options.environment_vars:
            error_details["environment_vars"] = {key: os.environ[key] for key in os.environ}
        if options.module_versions:
            error_details["module_versions"] = {module: sys.modules[module].__version__ if hasattr(sys.modules[module], '__version__') else 'N/A' for module in sys.modules}
    else:
        error_details["message"] = "Something went wrong."
    return error_details

# Custom JSON error handler
def json_error_handler(exc: BaseException) -> None:
    error_json = json.dumps(json_error_handler_dict(exc), indent=4)
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
        thread3.start_context = threading.current_thread().name
        thread3.start()
        thread3.join()

    def thread_level_1() -> None:
        thread2 = threading.Thread(target=thread_level_2, name="Thread-Level-2")
        thread2.start_context = threading.current_thread().name
        thread2.start()
        thread2.join()

    # Start the top-level thread
    print(f"-------------------In Thread-------------------")
    thread1 = threading.Thread(target=thread_level_1, name="Thread-Level-1")
    thread1.start_context = threading.current_thread().name
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
