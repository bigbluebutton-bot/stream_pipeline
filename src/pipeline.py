from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
import threading
from typing import Any, Callable, Dict, List, Tuple
import unittest
import uuid

from .module_classes import Module

@dataclass
class DataPackage:
    id: uuid.UUID
    pipeline_process_id: uuid.UUID
    sequence_number: int
    data: Any = None

class PipelineProcessingPhase:
    """
    Class to manage and execute a sequence of modules.
    """
    def __init__(self, modules: List[Module], name: str = "") -> None:
        self._id = "PPP-" + str(uuid.uuid4())
        if name == "":
            self._name = self._id
        else:
            self._name = name
        self._modules = modules.copy()

    def get_id(self) -> str:
        return self._id
    
    def get_name(self) -> str:
        return self._name

    def run(self, data_package: DataPackage) -> Tuple[bool, str, DataPackage]:
        """
        Executes the pipeline processing phase with the given data package.
        """
        data = data_package.data
        for module in self._modules:
            try:
                success, message, return_data = module.run(data)
                if not success:
                    return False, f"Module {module.get_name()} failed: {message}", data
            except Exception as e:
                return False, f"Module {module.get_name()} failed with error: {str(e)}", data

        data_package.data = return_data
        return True, f"Modules {module.get_name()} succeeded", data_package


class PipelineProcess:
    def __init__(self, name: str = "") -> None:
        self._id = "PP-" + str(uuid.uuid4())
        if name == "":
            self._name = self._id
        else:
            self._name = name
        self._next_sequence_number = 0
        self._last_finished_sequence_number = -1

        self._data_packages: Dict[int, DataPackage] = {}
        self._finished_data_packages: Dict[int, DataPackage] = {}

        self._lock = threading.Lock()

    def get_id(self) -> str:
        return self._id
    
    def get_name(self) -> str:
        return self._name
    
    def get_last_finished_sequence_number(self) -> int:
        return self._last_finished_sequence_number
    
    def set_last_finished_sequence_number(self, sequence_number: int) -> None:
        if sequence_number >= self._next_sequence_number or sequence_number < self._last_finished_sequence_number:
            raise("WARNING: Sequence number cannot be greater or equal than the next sequence number or smaller than the last finished sequence number. Ignoring.")
        self._last_finished_sequence_number = sequence_number
        
    
    def run(self, pipeline_processing_phases: List[PipelineProcessingPhase], sequence_number: int) -> Tuple[bool, str, DataPackage]:
        """
        Executes the pipeline process with the given data package.
        """
        with self._lock:
            if sequence_number not in self._data_packages:
                return False, f"Data package with sequence number {sequence_number} not found. First add data with add_data(data: Any)", None

            data_package = self._data_packages[sequence_number]
        
        for phase in pipeline_processing_phases:
            # Execute the phase with the data package
            success, message, result = phase.run(data_package)
            if not success:
                return False, f"Pipeline {self._name} failed: {message}", result

        return True, "All pipeline phases succeeded", data_package

    def add_data(self, data: Any) -> DataPackage:
        with self._lock:
            data_package = DataPackage(
                id= "DP-" + str(uuid.uuid4()),
                pipeline_process_id=self._id,
                sequence_number=self._next_sequence_number,
                data=data
            )
            self._data_packages[self._next_sequence_number] = data_package
            self._next_sequence_number += 1
            
            return data_package
        
    def remove_data(self, sequence_number: int) -> None:
        with self._lock:
            if sequence_number in self._data_packages:
                self._data_packages.pop(sequence_number)
            if sequence_number in self._finished_data_packages:
                self._finished_data_packages.pop(sequence_number)

    def push_finished_data_package(self, sequence_number: int) -> None:
        """
        Pushes a data package to the finished queue based on its sequence number.
        """
        with self._lock:
            if sequence_number in self._data_packages:
                data_package = self._data_packages.pop(sequence_number)
                self._finished_data_packages[sequence_number] = data_package

    def pop_finished_data_packages(self) -> Dict[uuid.UUID, DataPackage]:
        """
        Returns a dict of finished data packages in order of sequence number until the first missing sequence number and removes them from the queue.
        Example:
            Queue: [1, 2, 5, 6, 7]
                -> Returns: [1, 2]
            Queue left: [5, 6, 7]
        """
        with self._lock:
            finished_data_packages = {}
            current_sequence = self._last_finished_sequence_number + 1

            while current_sequence in self._finished_data_packages:
                data_package = self._finished_data_packages.pop(current_sequence)
                finished_data_packages[data_package.id] = data_package
                self._last_finished_sequence_number = current_sequence
                current_sequence += 1

            return finished_data_packages


class PipelineMode(Enum):
    ORDER_BY_SEQUENCE = 1
    FIRST_WINS = 2
    NO_ORDER = 3


class Pipeline:
    """
    Class to manage pre-processing, main processing, and post-processing stages.
    """
    def __init__(self, pre_modules: List[Module], main_modules: List[Module], post_modules: List[Module], name: str = "", max_workers: int = 10, mode: PipelineMode = PipelineMode.ORDER_BY_SEQUENCE) -> None:
        self._id: str = "P-" + str(uuid.uuid4())
        if name == "":
            self._name: str = self._id
        else:
            self._name: str = name
        self._pre_modules: PipelineProcessingPhase = PipelineProcessingPhase(pre_modules, name=self._name + "-pre")
        self._main_modules: PipelineProcessingPhase = PipelineProcessingPhase(main_modules, name=self._name + "-main")
        self._post_modules: PipelineProcessingPhase = PipelineProcessingPhase(post_modules, name=self._name + "-post")
        self._max_workers: int = max_workers
        self._mode: PipelineMode = mode
        self._pipeline_process_map: Dict[uuid.UUID, PipelineProcess] = {}
        self.active_futures: Dict[int, Future] = {}

        self._lock = threading.Lock()


    def get_id(self) -> str:
        return self._id
    
    def get_name(self) -> str:
        return self._name
    
    def set_pre_modules(self, modules: List[Module]) -> None:
        with self._lock:
            self._pre_modules = modules.copy()
            
    def set_main_modules(self, modules: List[Module]) -> None:
        with self._lock:
            self._main_modules = modules.copy()
            
    def set_post_modules(self, modules: List[Module]) -> None:
        with self._lock:
            self._post_modules = modules.copy()
            
    def set_max_workers(self, max_workers: int) -> None:
        with self._lock:
            self._max_workers = max_workers
            
    def set_mode(self, mode: PipelineMode) -> None:
        with self._lock:
            self._mode = mode
            
    def run(self, data: Any, callback: Callable[[bool, str, Any], None]) -> Tuple[bool, str, DataPackage]:
        """
        Executes the pipeline with the given data.
        """
        mode = self._mode
        
        callback_id = id(callback)
        with self._lock:
            process = self._pipeline_process_map.get(callback_id, None)
            if process is None:
                process = PipelineProcess(name=self._name + "-process-" + str(callback_id))
                self._pipeline_process_map[callback_id] = process
                
        data_package = process.add_data(data)
        
        def execute_pipeline():
            with self._lock:
                pipeline_processing_phases = [self._pre_modules, self._main_modules, self._post_modules]
            
            success, message, result = process.run(pipeline_processing_phases, data_package.sequence_number)
            if not success:
                callback(False, message, result)
                return
                
            if mode == PipelineMode.ORDER_BY_SEQUENCE:
                process.push_finished_data_package(data_package.sequence_number)
                finished_data_packages = process.pop_finished_data_packages()
                for _, finished_data_package in finished_data_packages.items():
                    callback(True, f"Pipeline {self._name} succeeded", finished_data_package)
                    
            elif mode == PipelineMode.FIRST_WINS:
                with self._lock:
                    last_finished_sequence_number = process.get_last_finished_sequence_number()
                    if data_package.sequence_number <= last_finished_sequence_number:
                        process.remove_data(data_package.sequence_number)
                        # Remove from thread queue if not already running
                        # callback(False, f"Data package with sequence number {data_package.sequence_number} is outdated, but was processed without errors", data_package)
                        return
                    process.set_last_finished_sequence_number(data_package.sequence_number)
                    callback(True, f"Pipeline {self._name} succeeded", data_package)
                    
            elif mode == PipelineMode.NO_ORDER:
                process.remove_data(data_package.sequence_number)
                callback(True, f"Pipeline {self._name} succeeded", finished_data_package)
                
        pid = process.get_id() + "-" + str(data_package.sequence_number)
        future = self.executor.submit(execute_pipeline)
        if mode == PipelineMode.FIRST_WINS:
            self.active_futures[pid] = future
        print(f"Task {pid} submitted")




class TestPipelineProcess(unittest.TestCase):
    def test_add_data(self) -> None:
        pp = PipelineProcess()
        pp.add_data("test_data_1")
        pp.add_data("test_data_2")

        self.assertEqual(len(pp._data_packages), 2)
        self.assertEqual(pp._next_sequence_number, 2)

    def test_push_finished_data_package(self) -> None:
        pp = PipelineProcess()
        pp.add_data("test_data_1")
        pp.add_data("test_data_2")

        pp.push_finished_data_package(0)
        pp.push_finished_data_package(1)

        self.assertEqual(len(pp._data_packages), 0)
        self.assertEqual(len(pp._finished_data_packages), 2)

    def test_pop_finished_data_packages(self) -> None:
        pp = PipelineProcess()
        pp.add_data("test_data_1")
        pp.add_data("test_data_2")
        pp.add_data("test_data_3")
        pp.push_finished_data_package(0)
        pp.push_finished_data_package(1)
        pp.push_finished_data_package(2)

        finished_packages = pp.pop_finished_data_packages()
        pp.pop_finished_data_packages()

        self.assertEqual(len(finished_packages), 3)
        self.assertEqual(len(pp._finished_data_packages), 0)
        self.assertEqual(pp._last_finished_sequence_number, 2)

    def test_pop_partial_finished_data_packages(self) -> None:
        pp = PipelineProcess()
        pp.add_data("test_data_1")
        pp.add_data("test_data_2")
        pp.add_data("test_data_3")
        pp.push_finished_data_package(0)
        pp.push_finished_data_package(2)

        finished_packages = pp.pop_finished_data_packages()
        pp.pop_finished_data_packages()

        self.assertEqual(len(finished_packages), 1)
        self.assertEqual(len(pp._finished_data_packages), 1)
        self.assertEqual(pp._last_finished_sequence_number, 0)
        self.assertIn(2, pp._finished_data_packages)


if __name__ == "__main__":
    unittest.main()