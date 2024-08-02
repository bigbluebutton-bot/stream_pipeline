

from concurrent.futures import Future, ThreadPoolExecutor
import time
from .module_classes import Module
import threading
from typing import Any, Callable, Dict, List, Sequence, Union
from enum import Enum
import uuid
from prometheus_client import Gauge, Summary

from .data_package import DataPackage, DataPackagePhase, DataPackagePhaseExecution

PIPELINE_ERROR_COUNTER = Gauge('pipeline_error_counter', 'Number of errors in the pipeline', ['pipeline_name'])
PIPELINE_PROCESSING_COUNTER = Gauge('pipeline_processing_counter', 'Number of processes executing the pipline at the moment', ['pipeline_name'])
PIPELINE_PROCESSING_TIME = Summary('pipeline_processing_time', 'Time spent processing the pipeline', ['pipeline_name'])
PIPELINE_PROCESSING_TIME_WITHOUT_ERROR = Summary('pipeline_processing_time_without_error', 'Time spent processing the pipeline without error', ['pipeline_name'])
PIPELINE_WAITING_COUNTER = Gauge('pipeline_waiting_counter', 'Number of processes waiting for the pipline to be executed', ['pipeline_name'])
PIPELINE_WAITING_TIME = Summary('pipeline_waiting_time', 'Time spent waiting for the pipeline to be executed', ['pipeline_name'])



class PhaseExecutionMode(Enum):
    """
    Enum to define different modes of pipeline execution.
    Values:
        NOT_PARALLEL: (default) Executes the pipeline in a single thread. Most likely the main thread. Worker will can be set to 0 or 1.
        ORDER_BY_SEQUENCE:      Orders at the end all DataPackages by how they were added at the beginning of the pipeline.
        FIRST_WINS:             First DataPackage that finishes is the one that is returned. All others are discarded.
        NO_ORDER:               No specific order for the DataPackages. They are returned as they finish.
    """
    NOT_PARALLEL = 0
    ORDER_BY_SEQUENCE = 1
    FIRST_WINS = 2
    NO_ORDER = 3

def phase_execution_mode_to_str(mode: PhaseExecutionMode) -> str:
    if mode == PhaseExecutionMode.NOT_PARALLEL:
        return "NOT_PARALLEL"
    elif mode == PhaseExecutionMode.ORDER_BY_SEQUENCE:
        return "ORDER_BY_SEQUENCE"
    elif mode == PhaseExecutionMode.FIRST_WINS:
        return "FIRST_WINS"
    elif mode == PhaseExecutionMode.NO_ORDER:
        return "NO_ORDER"
    else:
        return "UNKNOWN"

class PipelinePhase:
    def __init__(self, modules: List[Module], name: str = "") -> None:
        self._id: str = f"PP-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._modules: List[Module] = modules

        self._lock = threading.Lock()

    def execute(self, data_package: DataPackage, data_package_executor: DataPackagePhaseExecution) -> None:
        start_time = time.time()

        with self._lock:
            dp_phase = DataPackagePhase(
                running=True,
                start_time=start_time,
            )
        data_package_executor.phases.append(dp_phase)

        for module in self._modules:
            module.run(data_package=data_package, phase=dp_phase)
            if not data_package.success:
                break

        ent_time = time.time()
        dp_phase.running = False
        dp_phase.end_time = ent_time
        dp_phase.processing_time = ent_time - start_time

    def __deepcopy__(self, memo: Dict) -> 'PipelinePhase':
        copied_phase = PipelinePhase(
            modules=[module.__deepcopy__(memo) for module in self._modules],
            name=self._name
        )
        copied_phase._id = self._id
        return copied_phase

class OrderTracker:
    def __init__(self) -> None:
        self._next_sequence_number = 0
        self._last_finished_sequence_number = -1
        self._data_packages: Dict[int, DataPackage] = {}
        self._finished_data_packages: Dict[int, DataPackage] = {}

        self._lock = threading.Lock()

    def get_last_finished_sequence_number(self) -> int:
        """
        Returns the last finished sequence number.
        """
        return self._last_finished_sequence_number
    
    def get_next_sequence_number(self) -> int:
        with self._lock:
            return self._next_sequence_number

    def set_last_finished_sequence_number(self, sequence_number: int) -> None:
        """
        Sets the last finished sequence number.
        Args:
            sequence_number (int): The sequence number to set as the last finished.
        """
        if sequence_number >= self._next_sequence_number or sequence_number < self._last_finished_sequence_number:
            raise ValueError("Sequence number cannot be greater or equal than the next sequence number or smaller than the last finished sequence number.")
        self._last_finished_sequence_number = sequence_number

    def add_data(self, dp: DataPackage) -> None:
        with self._lock:
            self._data_packages[self._next_sequence_number] = dp
            self._next_sequence_number += 1
    
    def remove_data(self, sequence_number: int) -> None:
        """
        Removes a data package based on its sequence number.
        Args:
            sequence_number (int): The sequence number of the data package to remove.
        """
        with self._lock:
            self._data_packages.pop(sequence_number, None)
            self._finished_data_packages.pop(sequence_number, None)

    def push_finished_data_package(self, sequence_number: int) -> None:
        """
        Pushes a data package to the finished queue based on its sequence number.
        Args:
            sequence_number (int): The sequence number of the data package to push to the finished queue.
        """
        with self._lock:
            if sequence_number in self._data_packages:
                data_package = self._data_packages.pop(sequence_number)
                self._finished_data_packages[sequence_number] = data_package
            else:
                raise ValueError("Sequence number not found in data packages.")

    def pop_finished_data_packages(self) -> List[DataPackage]:
        """
        Returns a dict of finished data packages in order of sequence number until the first missing sequence number and removes them from the queue.
        Example:
            Queue: [1, 2, 5, 6, 7]
                -> Returns: [1, 2]
            Queue left: [5, 6, 7]
        Returns:
            List[DataPackage]: List of finished data packages.
        """
        with self._lock:
            finished_data_packages: List[DataPackage] = []
            current_sequence = self._last_finished_sequence_number + 1

            while current_sequence in self._finished_data_packages:
                data_package = self._finished_data_packages.pop(current_sequence)
                finished_data_packages.append(data_package)
                self._last_finished_sequence_number = current_sequence
                current_sequence += 1

            return finished_data_packages

class PipelinePhaseExecution:
    def __init__(self, phases: List[PipelinePhase], name: str = "", max_workers: int = 1, mode: PhaseExecutionMode = PhaseExecutionMode.NOT_PARALLEL) -> None:
        self._id: str = f"PPE-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._phases: List[PipelinePhase] = phases
        self._mode: PhaseExecutionMode = mode

        if mode == PhaseExecutionMode.NOT_PARALLEL and max_workers > 1:
            max_workers = 1

        self._max_workers = max_workers

        self._executor = ThreadPoolExecutor(max_workers=max_workers) if max_workers > 0 else None
        self._active_futures: Dict[str, Future] = {}

        self._order_tracker = OrderTracker()

        self._lock = threading.Lock()

    def execute(self, instance_lock: threading.Lock, data_package: DataPackage, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:
        start_time = time.time()

        dp_phase_ex = DataPackagePhaseExecution(
            mode=phase_execution_mode_to_str(self._mode),
            workers=self._max_workers,
            sequence_number=self._order_tracker.get_next_sequence_number(),
            running=True,
            start_time=start_time,
        )

        # print(f"Starting {data_package.data} with sequence number {dp_phase_ex.sequence_number}")
        data_package.phases.append(dp_phase_ex)
        
        self._order_tracker.add_data(data_package)
        
        start_context = threading.current_thread().name

        waiting_time = 0.0

        def execute_phases() -> None:
            nonlocal waiting_time, start_context, dp_phase_ex, data_package, start_time, instance_lock
            try:
                start_processing_time = time.time()
                waiting_time = time.time() - start_time
                dp_phase_ex.waiting_time = waiting_time

                temp_phases = []
                with self._lock:
                    temp_phases = self._phases.copy()

                for phase in temp_phases:
                    phase.execute(data_package, dp_phase_ex)
                    if not data_package.success:
                        break

                with instance_lock:
                    if self._mode == PhaseExecutionMode.ORDER_BY_SEQUENCE:
                        # print(f"Finished: {data_package.data} with sequence number {dp_phase_ex.sequence_number}")
                        self._order_tracker.push_finished_data_package(dp_phase_ex.sequence_number)
                        finished_data_packages = self._order_tracker.pop_finished_data_packages()
                        # print(f"Finished data packages: {len(finished_data_packages)}")
                        for finished_data_package in finished_data_packages:
                            # print(f"{dp_phase_ex.sequence_number}")
                            if finished_data_package.success:
                                callback(finished_data_package)
                            else:
                                if error_callback:
                                    error_callback(finished_data_package)

                    elif self._mode == PhaseExecutionMode.FIRST_WINS:
                        with self._lock:
                            last_finished_sequence_number = self._order_tracker.get_last_finished_sequence_number()
                            if dp_phase_ex.sequence_number <= last_finished_sequence_number and data_package.success:
                                self._active_futures.pop(f"{self.get_id()}-{dp_phase_ex.sequence_number}")
                                self._order_tracker.remove_data(dp_phase_ex.sequence_number)
                                if not data_package.success:
                                    error_callback(data_package)
                            else:
                                self._order_tracker.set_last_finished_sequence_number(dp_phase_ex.sequence_number)
                                callback(data_package)

                    elif self._mode == PhaseExecutionMode.NO_ORDER:
                        self._order_tracker.remove_data(dp_phase_ex.sequence_number)
                        if data_package.success:
                            callback(data_package)
                        else:
                            if error_callback:
                                error_callback(data_package)

                    elif self._mode == PhaseExecutionMode.NOT_PARALLEL:
                        if data_package.success:
                            callback(data_package)
                        else:
                            if error_callback:
                                error_callback(data_package)

            except Exception as e:
                data_package.success = False
                data_package.errors.append(e)
                if error_callback:
                    error_callback(data_package)


            end_time = time.time()
            total_time = end_time - start_time
            processing_time = end_time - start_processing_time
            dp_phase_ex.running = False
            dp_phase_ex.end_time = end_time
            dp_phase_ex.processing_time = processing_time
            dp_phase_ex.total_time = total_time

            data_package.running = False

        
        if self._executor is None:
            execute_phases()
            return

        eid = f"{self.get_id()}-{dp_phase_ex.sequence_number}"
        future = self._executor.submit(execute_phases)
        if self._mode == PhaseExecutionMode.FIRST_WINS:
            self._active_futures[eid] = future
        


        
    def __deepcopy__(self, memo: Dict) -> 'PipelinePhaseExecution':
        copied_phase = PipelinePhaseExecution(
            name=self._name,
            phases=[phase.__deepcopy__(memo) for phase in self._phases],
            mode=self._mode,
            max_workers=self._max_workers,
        )
        copied_phase._id = self._id
        return copied_phase
    
    def get_id(self) -> str:
        with self._lock:
            return self._id

    def set_order_tracker(self, order_tracker: OrderTracker) -> None:
        with self._lock:
            self._order_tracker = order_tracker

class PipelineInstance:
    def __init__(self, name: str = "") -> None:
        self._id: str = f"PE-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._phases_execution_queue: Dict[str, List[PipelinePhaseExecution]] = {}

        self._lock = threading.Lock()
        self._execution_lock = threading.Lock()

    def execute(self, phases: List[PipelinePhaseExecution], dp: DataPackage, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:      
        dp.pipeline_executer_id = self._id

        self._phases_execution_queue[dp.id] = phases.copy()

        def new_callback(dp: DataPackage) -> None:
            nonlocal callback, error_callback
            left_phases = []
            for phase in self._phases_execution_queue[dp.id]:
                left_phases.append(phase._name)

            # print(f"{dp.data} {len(dp.phases)}/{len(phases)}({len(self._phases_execution_queue[dp.id])}) {left_phases}")

            if not dp.success and error_callback:
                del self._phases_execution_queue[dp.id]
                error_callback(dp)
                return

            if len(self._phases_execution_queue[dp.id]) > 0:
                phase = self._phases_execution_queue[dp.id].pop(0)
                phase.execute(self._execution_lock, dp, new_callback, error_callback)
                # print(f"Task {dp.data} submitted to {phase._name}. Remaining tasks: {len(phases_queue)}")
                return
            
            del self._phases_execution_queue[dp.id]
            callback(dp)

        new_callback(dp)

    def get_id(self) -> str:
        with self._lock:
            return self._id
        
    def get_name(self) -> str:
        with self._lock:
            return self._name

class Pipeline:
    def __init__(self, phases: Union[Sequence[Union[PipelinePhaseExecution, PipelinePhase]], None] = None, name: str = "") -> None:
        self._id: str = f"P-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._phases: List[PipelinePhaseExecution] = []
        self._instances_phases: Dict[str, List[PipelinePhaseExecution]] = {} # Each instance has its own copy of the phases
        self._pipeline_instances: Dict[str, PipelineInstance] = {} # Dict of instances

        self._lock = threading.Lock()

        self.set_phases(phases)

    def get_id(self) -> str:
        with self._lock:
            return self._id
        
    def get_name(self) -> str:
        with self._lock:
            return self._name

    def set_phases(self, phases: Union[Sequence[Union[PipelinePhaseExecution, PipelinePhase]], None] = None) -> None:
        """
        This will set the phases for the pipeline and create a deepcopy for each phase and modules.
        """

        if phases:
            temp_phases = []
            for phase in phases:
                if isinstance(phase, PipelinePhase):
                    phase_list = [phase]
                    temp_phases.append(PipelinePhaseExecution(phase_list))
                else:
                    temp_phases.append(phase)
        
            with self._lock:
                for phase in temp_phases:
                    self._phases.append(phase.__deepcopy__({}))

        # for each instance create a deepcopy of the phases
        with self._lock:
            for ex_id in self._pipeline_instances:
                order_tracker = OrderTracker()
                for phase in self._phases:
                    copy_phase = phase.__deepcopy__({})
                    copy_phase.set_order_tracker(order_tracker)
                    self._instances_phases[ex_id].append(copy_phase)

    def register_instance(self) -> str:
        ex = PipelineInstance(name=f"")
        with self._lock:
            self._pipeline_instances[ex.get_id()] = ex
            self._instances_phases[ex.get_id()] = []
        self.set_phases()
        return ex._id

    def _get_instance(self, instance_id: str) -> Union[PipelineInstance, None]:
        with self._lock:
            return self._pipeline_instances.get(instance_id, None)
    
    def unregister_instance(self, ex_id: str) -> None:
        ex = self._get_instance(ex_id)
        if ex:
            with self._lock:
                del self._pipeline_instances[ex_id]
                del self._instances_phases[ex_id]

    def execute(self, data: Any, instance_id: str, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:
        ex = self._get_instance(instance_id)
        if not ex:
            raise ValueError("Instance ID not found")
        
        temp_phases = []
        with self._lock:
            temp_phases = self._instances_phases.get(instance_id, []).copy()

        # Put data into DataPackage
        dp = DataPackage(
            pipeline_id=self._id,
            pipeline_executer_id=instance_id,
            data=data
        )

        dp.running = True
        ex.execute(temp_phases, dp, callback, error_callback)