

from concurrent.futures import Future, ThreadPoolExecutor
import time
from .module_classes import Module
import threading
from typing import Any, Callable, Dict, List, Union
from enum import Enum
import uuid
from prometheus_client import Gauge, Summary

from .data_package import DataPackage, DataPackagePhase

PIPELINE_ERROR_COUNTER = Gauge('pipeline_error_counter', 'Number of errors in the pipeline', ['pipeline_name'])
PIPELINE_PROCESSING_COUNTER = Gauge('pipeline_processing_counter', 'Number of processes executing the pipline at the moment', ['pipeline_name'])
PIPELINE_PROCESSING_TIME = Summary('pipeline_processing_time', 'Time spent processing the pipeline', ['pipeline_name'])
PIPELINE_PROCESSING_TIME_WITHOUT_ERROR = Summary('pipeline_processing_time_without_error', 'Time spent processing the pipeline without error', ['pipeline_name'])
PIPELINE_WAITING_COUNTER = Gauge('pipeline_waiting_counter', 'Number of processes waiting for the pipline to be executed', ['pipeline_name'])
PIPELINE_WAITING_TIME = Summary('pipeline_waiting_time', 'Time spent waiting for the pipeline to be executed', ['pipeline_name'])



class ParallelExecutionMode(Enum):
    """
    Enum to define different modes of pipeline execution.
    Values:
        ORDER_BY_SEQUENCE: Orders at the end all DataPackages by how they were added at the beginning of the pipeline.
        FIRST_WINS: First DataPackage that finishes is the one that is returned. All others are discarded.
        NO_ORDER: No specific order for the DataPackages. They are returned as they finish.
    """
    ORDER_BY_SEQUENCE = 1
    FIRST_WINS = 2
    NO_ORDER = 3



class PipelinePhase:
    def __init__(self, modules: List[Module], name: str = "") -> None:
        self._id: str = f"PP-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._modules: List[Module] = modules

        self._lock = threading.Lock()

    def execute(self, data_package: DataPackage) -> None:
        start_time = time.time()

        with self._lock:
            dp_phase = DataPackagePhase(
                running=True,
                start_time=start_time,
            )
        data_package.phases.append(dp_phase)

        for module in self._modules:
            module.run(data_package=data_package, phase=dp_phase)
            if not data_package.success:
                break

        ent_time = time.time()
        dp_phase.running = False
        dp_phase.end_time = ent_time
        dp_phase.processing_time = ent_time - start_time


class ParallelPhaseExecution:
    def __init__(self, phases: List[PipelinePhase], mode: ParallelExecutionMode, name: str = "", max_workers: int = 10) -> None:
        self._id: str = f"PPE-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._phases: List[PipelinePhase] = phases
        self._mode: ParallelExecutionMode = mode
        self._max_workers: int = max_workers

        self._lock = threading.Lock()

    def execute(self, data_package: DataPackage) -> None:
        start_time = time.time()

        with self._lock:
            dp_phase = DataPackagePhase(
                running=True,
                start_time=start_time,
            )
        data_package.phases.append(dp_phase)

        futures = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            for phase in self._phases:
                future = executor.submit(phase.execute, data_package)
                futures.append(future)
            
            if self._mode == ParallelExecutionMode.FIRST_WINS:
                for future in futures:
                    if future.done():
                        break
            elif self._mode == ParallelExecutionMode.ORDER_BY_SEQUENCE:
                for future in futures:
                    future.result()
            elif self._mode == ParallelExecutionMode.NO_ORDER:
                for future in futures:
                    future.result()

        if data_package.success:
            end_time = time.time()
            with self._lock:
                dp_phase.running = False
                dp_phase.end_time = end_time
                dp_phase.duration = end_time - start_time
                data_package.phases[-1] = dp_phase

class PipelineExecutor:
    def __init__(self, name: str = "") -> None:
        self._id: str = f"PE-{uuid.uuid4()}"
        self._name: str = name if name else self._id

        self._lock = threading.Lock()

    def execute(self, phases_or_rule: Union[List[PipelinePhase], ParallelPhaseExecution], dp: DataPackage, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:      
        dp.pipeline_executer_id = self._id
        if isinstance(phases_or_rule, ParallelPhaseExecution):
            phases_or_rule.execute(dp)
        elif isinstance(phases_or_rule, list):
            for phase in phases_or_rule:
                phase.execute(dp)
                if not dp.success:
                    break

        dp.running = False

        if dp.success:
            callback(dp)
        else:
            if error_callback:
                error_callback(dp)

    def get_id(self) -> str:
        with self._lock:
            return self._id
        
    def get_name(self) -> str:
        with self._lock:
            return self._name

class Pipeline:
    # List of Phaseobjects to pretend that the pipeline is a list of new phases and this phase is not in any other pipeline
    _phases_list: List[PipelinePhase] = [] # static
    _phases_list_lock = threading.Lock() # static

    def __init__(self, phases: List[PipelinePhase], name: str = "") -> None:
        self._id: str = f"P-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._phases: List[PipelinePhase] = []
        self._pipeline_executors: Dict[str, PipelineExecutor] = {}

        self._lock = threading.Lock()

        self.set_phases(phases)

    def get_id(self) -> str:
        with self._lock:
            return self._id
        
    def get_name(self) -> str:
        with self._lock:
            return self._name

    def set_phases(self, phases: List[PipelinePhase]) -> None:
        with self._phases_list_lock:
            for phase in phases:
                if phase in self._phases_list:
                    raise ValueError("Phases are not unique. Create new phases obj for each pipeline to prevent hard to debug errors.")
                else:
                    self._phases_list.append(phase)
        
        with self._lock:
            self._phases = phases

    def register_executor(self) -> str:
        ex = PipelineExecutor(name=f"")
        with self._lock:
            self._pipeline_executors[ex.get_id()] = ex
            return ex._id

    def _get_executor(self, ex_id: str) -> Union[PipelineExecutor, None]:
        with self._lock:
            return self._pipeline_executors.get(ex_id, None)
    
    def unregister_executor(self, ex_id: str) -> None:
        ex = self._get_executor(ex_id)
        if ex:
            # ex.stop() # TODO: Implement stop method
            with self._lock:
                del self._pipeline_executors[ex_id]

    def execute(self, data: Any, executor_id: str, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:
        ex = self._get_executor(executor_id)
        if not ex:
            raise ValueError("Executor ID not found")
        
        temp_phases = []
        with self._lock:
            temp_phases = self._phases.copy()

        # Put data into DataPackage
        dp = DataPackage(
            pipeline_id=self._id,
            pipeline_executer_id=executor_id,
            data=data
        )

        dp.running = True
        ex.execute(temp_phases, dp, callback, error_callback)
        dp.running = False