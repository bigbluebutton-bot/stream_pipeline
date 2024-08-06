# import copy
# from stream_pipeline.data_package import DataPackage, testclass



# test1 = testclass(MyDataType1)
# print(test1)
# test1.id = "some_id"
# test2 = copy.deepcopy(test1)
# test1.id = "some"
# print(test1)
# print(test2)

# test1._data = MyDataType1(1)

from dataclasses import InitVar, dataclass, field
from abc import ABC
from typing import List

from stream_pipeline.thread_safe_class import ThreadSafeClass


from dataclasses import dataclass, field
from typing import Generic, Optional, TypeVar

T = TypeVar('T')

@dataclass
class testclass(Generic[T], ThreadSafeClass):
    _id: str = field(default_factory=lambda: "test", init=False)
    _data: Optional[T] = None
    _ThreadSafeClass__immutable_attributes: List[str] = field(default_factory=lambda: ['id'])
    
    @property
    def id(self) -> str:
        return self._get_attribute('id')
    
    @id.setter
    def id(self, value: str) -> None:
        self._set_attribute('id', value)
        
    @property
    def data(self) -> Optional[T]:
        return self._get_attribute('data')
    
    @data.setter
    def data(self, value: T) -> None:
        self._set_attribute('data', value)
        
class MyDataType1:
    def __init__(self, some_property: int):
        self.some_property = some_property
        
# Correct instantiation of test1 with the generic type MyDataType1
test1 = testclass[MyDataType1]()
test1.data = MyDataType1(1)

print(test1)  # This should print: 1
