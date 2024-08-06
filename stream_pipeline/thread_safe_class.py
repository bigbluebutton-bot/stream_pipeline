from abc import ABC
import json
import threading
import copy
from typing import Any, Dict, List, final
from dataclasses import dataclass, field

@dataclass
class ThreadSafeClass(ABC):
    __mutex: threading.Lock = field(default_factory=threading.Lock, init=False)  # For locking all properties that start with '_'
    __mutexes: Dict[str, threading.Lock] = field(default_factory=dict, init=False)
    __immutable_attributes: List[str] = field(default_factory=list, init=False)

    @final
    def __post_init__(self):
        for attr in self.__dict__:
            if not attr.startswith('__') and not attr.startswith('_ThreadSafeClass'):
                self.__mutexes[attr] = threading.Lock()

    def _get_attribute(self, name: str) -> Any:
        new_name = "_" + name
        if name.startswith('__'):
            with self.__mutex:
                return self.__dict__[new_name]
        else:
            if name not in self.__mutexes:
                self.__mutexes[name] = threading.Lock()
            
            with self.__mutexes[name]:
                return self.__dict__[new_name]

    def _set_attribute(self, name: str, value: Any) -> None:
        new_name = "_" + name
        if name in self.__immutable_attributes and new_name in self.__dict__:
            raise AttributeError(f"'{self.__class__.__name__}' object attribute '{name}' is immutable")
        
        if name.startswith('__'):
            with self.__mutex:
                self.__dict__[new_name] = value
        else:
            if name not in self.__mutexes:
                self.__mutexes[name] = threading.Lock()
            
            with self.__mutexes[name]:
                current_thread = threading.current_thread()
                if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                    raise RuntimeError("Modification not allowed: the thread handling this DataPackage has timed out.")
                
                self.__dict__[new_name] = value

    def __deepcopy__(self, memo: Dict[int, Any]) -> Any:
        # TODO: Block acces to data, when deepcopy is called
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in self.__dict__.items():
            if not k.startswith('_ThreadSafeClass') or k == '_ThreadSafeClass__immutable_attributes':
                setattr(result, k, copy.deepcopy(v, memo))

        result.__mutexes = {k: threading.Lock() for k in self.__dict__.keys() if not k.startswith('__')}
        return result

    def to_dict(self) -> Dict[str, Any]:
        def process_dict(data: Any) -> Any:
            if isinstance(data, dict):
                return {k: process_dict(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple, set)):
                return type(data)(process_dict(item) for item in data)
            elif isinstance(data, BaseException):
                from .error import json_error_handler_dict
                return json_error_handler_dict(data)
            elif hasattr(data, 'to_dict'):
                print(getattr(data, 'to_dict'))
                return data.to_dict()
            else:
                return data

        result = {}
        for key, value in self.__dict__.items():
            if key.endswith('__immutable_attributes') or key.startswith('_ThreadSafeClass') or key == '__orig_class__':
                continue
            if key.startswith('_'):
                key = key[1:]
                result[key] = process_dict(value)
        return result

    def __str__(self) -> str:
        data_dict = self.to_dict()
        try:
            jsonstring = json.dumps(data_dict, default=str, indent=4)
        except (TypeError, ValueError):
            data_dict['data'] = "Data which cannot be displayed as JSON"
            jsonstring = json.dumps(data_dict, default=str, indent=4)

        return jsonstring