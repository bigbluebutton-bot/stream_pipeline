import json
import types
import threading
import copy
from typing import Any, Dict, List
from dataclasses import dataclass, field

@dataclass
class ThreadSafeClass:
    _mutex: threading.Lock = threading.Lock()  # For locking all properties that start with '_'
    _mutexes: Dict[str, threading.Lock] = field(default_factory=dict, init=False)
    _immutable_attributes: List[str] = field(default_factory=list)

    def __getattribute__(self, name: str) -> Any:
        if name == '_mutex':
            return super().__getattribute__(name)

        if name.startswith('_'):
            with self._mutex:
                return super().__getattribute__(name)
        attr = super().__getattribute__(name)
        if isinstance(attr, types.MethodType):
            return attr

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

        if name.startswith('_'):
            with self._mutex:
                super().__setattr__(name, value)
                return

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
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in self.__dict__.items():
            if k != '_mutexes':
                setattr(result, k, copy.deepcopy(v, memo))

        result._mutexes = {k: threading.Lock() for k in self.__dict__.keys() if k != '_mutexes'}
        return result

    def copy(self):
        return copy.deepcopy(self)
    
    def to_dict(self):
        def process_dict(data):
            if isinstance(data, dict):
                return {k: process_dict(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple, set)):
                return type(data)(process_dict(item) for item in data)
            elif isinstance(data, BaseException):
                from .error import json_error_handler_dict
                return json_error_handler_dict(data)
            elif hasattr(data, 'to_dict'):
                return data.to_dict()
            else:
                return data
        
        result = {}
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            result[key] = process_dict(value)
        return result

    def __str__(self):
        data_dict = self.to_dict()
        # Handle non-serializable data
        try:
            jsonstring = json.dumps(data_dict, default=str, indent=4)
        except (TypeError, ValueError):
            data_dict['data'] = "Data which cannot be displayed as JSON"
            jsonstring = json.dumps(data_dict, default=str, indent=4)

        return jsonstring