

from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import time

from stream_pipeline.error import exception_to_error
from .module_classes import Module
import threading
from typing import Callable, Dict, Generic, List, Optional, Sequence, Tuple, TypeVar, Union
from enum import Enum
import uuid
from prometheus_client import Gauge, Summary, Counter

from .data_package import DataPackage, DataPackageModule, DataPackagePhase, DataPackageController, Status

PIPELINE_INPUT_FLOWRATE = Counter("pipeline_input_flowrate", "The flowrate of the pipeline input", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])
PIPELINE_OUTPUT_FLOWRATE = Counter("pipeline_output_flowrate", "The flowrate of the pipeline output", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])
PIPELINE_EXIT_FLOWRATE = Counter("pipeline_exit_flowrate", "The flowrate of the pipeline exit", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])
PIPELINE_OVERFLOW_FLOWRATE = Counter("pipeline_overflow_flowrate", "The flowrate of the pipeline overflow", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])
PIPELINE_OUTDATED_FLOWRATE = Counter("pipeline_outdated_flowrate", "The flowrate of DP which are outdated", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])
PIPELINE_ERROR_FLOWRATE = Counter("pipeline_error_flowrate", "The flowrate of the pipeline error", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])

PIPELINE_TOTAL_TIME = Summary("pipeline_total_time", "The total time of the pipeline", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])
PIPELINE_PROCESSING_COUNT = Gauge("pipeline_processing_count", "The number of data packages being processed", ["pipeline_name", "pipeline_id", "pipeline_instance_id"])


CONTROLLER_INPUT_FLOWRATE = Counter("controller_input_flowrate", "The flowrate of the controller input", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_OUTPUT_FLOWRATE = Counter("controller_output_flowrate", "The flowrate of the controller output", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_EXIT_FLOWRATE = Counter("controller_exit_flowrate", "The flowrate of the controller exit", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_OVERFLOW_FLOWRATE = Counter("controller_overflow_flowrate", "The flowrate of the controller overflow", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_OUTDATED_FLOWRATE = Counter("controller_outdated_flowrate", "The flowrate of DP which are outdated", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_ERROR_FLOWRATE = Counter("controller_error_flowrate", "The flowrate of the controller error", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])

CONTROLLER_INPUT_WAITING_TIME = Summary("controller_input_waiting_time", "The waiting time of the controller input", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_OUTPUT_WAITING_TIME = Summary("controller_output_waiting_time", "The waiting time of the controller output", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_TOTAL_TIME = Summary("controller_total_time", "The total time of the controller", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])

CONTROLLER_INPUT_WAITING_COUNTER = Gauge("controller_input_waiting_counter", "The number of data packages waiting to be processed", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_OUTPUT_WAITING_COUNTER = Gauge("controller_output_waiting_counter", "The number of data packages waiting to be output", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])
CONTROLLER_PROCESSING_COUNT = Gauge("controller_processing_count", "The number of data packages being processed", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id"])


PHASE_INPUT_FLOWRATE = Counter("phase_input_flowrate", "The flowrate of the phase input", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id"])
PHASE_OUTPUT_FLOWRATE = Counter("phase_output_flowrate", "The flowrate of the phase output", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id"])
PHASE_EXIT_FLOWRATE = Counter("phase_exit_flowrate", "The flowrate of the phase exit", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id"])
PHASE_ERROR_FLOWRATE = Counter("phase_error_flowrate", "The flowrate of the phase error", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id"])

PHASE_TOTAL_TIME = Summary("phase_total_time", "The total time of the phase", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id"])

PHASE_PROCESSING_COUNT = Gauge("phase_processing_count", "The number of data packages being processed", ["pipeline_name", "pipeline_id", "pipeline_instance_id", "controller_name", "controller_id", "phase_name", "phase_id"])

class ControllerMode(Enum):
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

def controller_mode_to_str(mode: ControllerMode) -> str:
    if mode == ControllerMode.NOT_PARALLEL:
        return "NOT_PARALLEL"
    elif mode == ControllerMode.ORDER_BY_SEQUENCE:
        return "ORDER_BY_SEQUENCE"
    elif mode == ControllerMode.FIRST_WINS:
        return "FIRST_WINS"
    elif mode == ControllerMode.NO_ORDER:
        return "NO_ORDER"
    else:
        return "UNKNOWN"

class PipelinePhase:
    def __init__(self, name: str, modules: List[Module]) -> None:
        self._id: str = f"PP-{uuid.uuid4()}"
        self._name: str = name
        self._modules: List[Module] = modules

        self._lock = threading.Lock()

    def execute(self, data_package: DataPackage, data_package_controller: DataPackageController) -> DataPackagePhase:
        start_time = time.time()
        with self._lock:
            PHASE_INPUT_FLOWRATE.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).inc()
            PHASE_PROCESSING_COUNT.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).inc()
        
        dp_phase = DataPackagePhase()
        with self._lock:
            dp_phase.phase_id=self._id
            dp_phase.phase_name=self._name
        dp_phase.status = Status.RUNNING
        dp_phase.start_time=start_time
            
        data_package_controller.phases.append(dp_phase)

        for module in self._modules:
            dpm = module.run(dp=data_package, dpc=data_package_controller, dpp=dp_phase)
            if not dpm.status == Status.SUCCESS:
                dp_phase.status = dpm.status
                break

        if dp_phase.status == Status.RUNNING:
            dp_phase.status = Status.SUCCESS

        ent_time = time.time()
        dp_phase.end_time = ent_time
        total_time = ent_time - start_time
        dp_phase.total_time = total_time

        with self._lock:
            if dp_phase.status == Status.SUCCESS:
                PHASE_OUTPUT_FLOWRATE.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).inc()
            elif dp_phase.status == Status.EXIT:
                PHASE_EXIT_FLOWRATE.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).inc()
            elif dp_phase.status == Status.ERROR:
                PHASE_ERROR_FLOWRATE.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).inc
            else:
                try:
                    raise ValueError("Status not recognized.")
                except ValueError as e:
                    dp_phase.status = Status.ERROR
                    data_package.errors.append(exception_to_error(e))
                    
            
            PHASE_PROCESSING_COUNT.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).dec()
            PHASE_TOTAL_TIME.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, data_package_controller.controller_name, data_package_controller.controller_id, self._name, self._id).observe(total_time)
            
        return dp_phase

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
        self._data_packages: Dict[int, Optional[Tuple[DataPackage, DataPackageController]]] = {}
        self._finished_data_packages: Dict[int, Optional[Tuple[DataPackage, DataPackageController]]] = {}

        self._lock = threading.Lock()

        self.instance_lock = threading.Lock()

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

    def add_data(self, dp: DataPackage, dpc: DataPackageController) -> None:
        with self._lock:
            self._data_packages[self._next_sequence_number] = (dp, dpc)
            self._next_sequence_number += 1
    
    def remove_data(self, sequence_number: int) -> None:
        """
        Removes a data package based on its sequence number.
        Args:
            sequence_number (int): The sequence number of the data package to remove.
        """
        with self._lock:
            if sequence_number in self._data_packages:
                self._data_packages.pop(sequence_number)
                self._finished_data_packages[sequence_number] = None
            else:
                raise ValueError("Sequence number not found in data packages.")
            

    def push_finished_data_package(self, sequence_number: int) -> None:
        """
        Pushes a data package to the finished queue based on its sequence number.
        Args:
            sequence_number (int): The sequence number of the data package to push to the finished queue.
        """
        with self._lock:
            if sequence_number in self._data_packages:
                dp_and_dpc_or_none = self._data_packages.pop(sequence_number)
                self._finished_data_packages[sequence_number] = dp_and_dpc_or_none
            else:
                raise ValueError("Sequence number not found in data packages.")

    def pop_finished_data_packages(self, mode: ControllerMode) -> Tuple[List[Tuple[DataPackage, DataPackageController]], List[Tuple[DataPackage, DataPackageController]]]:
        """
        NO_ORDER:
            Example:
                Queue: [1, 2, 5, 6, 7]
                    -> Returns: [1, 2, 5, 6, 7]
                Queue left: []

        ORDER_BY_SEQUENCE or NOT_PARALLEL:
            Example:
                Queue: [1, 2, 5, 6, 7]
                    -> Returns: [1, 2]
                Queue left: [5, 6, 7]

        FIRST_WINS:
            Example:
                last finished: 4
                Queue: [1, 2, 5, 6, 7]
                    -> Returns: [5, 6, 7]
                Queue left: []
        """
        with self._lock:
            finished_data_packages: List[Tuple[DataPackage, DataPackageController]] = []
            outdated_data_packages: List[Tuple[DataPackage, DataPackageController]] = []

            if (mode == ControllerMode.NO_ORDER):
                for sequence_number, dp_and_dpc in self._finished_data_packages.items():
                    if dp_and_dpc is None:
                        continue
                    finished_data_packages.append(dp_and_dpc)
                self._finished_data_packages = {}
                    
            elif mode == ControllerMode.ORDER_BY_SEQUENCE or mode == ControllerMode.NOT_PARALLEL:
                current_sequence = self._last_finished_sequence_number + 1

                while current_sequence in self._finished_data_packages:
                    dp_and_dpc = self._finished_data_packages.pop(current_sequence)
                    self._last_finished_sequence_number = current_sequence
                    current_sequence = current_sequence + 1
                    if dp_and_dpc is None:
                        continue
                    finished_data_packages.append(dp_and_dpc)

            elif mode == ControllerMode.FIRST_WINS:
                # find each dp which has a bigger sequence number than the last finished sequence number
                last_sequence_number = self._last_finished_sequence_number + 1
                for sequence_number, dp_and_dpc in self._finished_data_packages.items():
                    if dp_and_dpc is None:
                        continue
                    
                    if sequence_number >= last_sequence_number:
                        self._last_finished_sequence_number = sequence_number
                        last_sequence_number = sequence_number + 1
                        finished_data_packages.append(dp_and_dpc)
                    else:
                        outdated_data_packages.append(dp_and_dpc)

                # remove the data packages from the finished queue
                self._finished_data_packages.clear()
                
            return finished_data_packages, outdated_data_packages

@dataclass
class QueueData:
    start_context: str
    dp_phase_con: DataPackageController
    data_package: DataPackage
    start_time: float

class PipelineController:
    def __init__(self, name: str, phases: List[PipelinePhase], max_workers: int = 1, queue_size: int = -1, mode: ControllerMode = ControllerMode.NOT_PARALLEL) -> None:
        self._id: str = f"C-{uuid.uuid4()}"
        self._name: str = name
        self._phases: List[PipelinePhase] = phases
        self._mode: ControllerMode = mode

        if queue_size < 0:
            queue_size = max_workers

        if (mode == ControllerMode.NOT_PARALLEL and max_workers > 1) or max_workers < 1:
            max_workers = 1

        self._max_workers = max_workers

        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        self._order_tracker = OrderTracker()

        self._queue_size = queue_size
        self._dp_queue: deque = deque(maxlen=queue_size) # Hase data in it of type QueueData
        self._dp_queue_lock = threading.Lock()

        self._lock = threading.Lock()

    def execute(self, data_package: DataPackage, callback: Callable[[DataPackage, Optional[DataPackageController]], None], overflow_callback: Callable[[DataPackage], None], outdated_callback: Callable[[DataPackage], None]) -> None:
        start_time = time.time()
        with self._lock:
            CONTROLLER_INPUT_FLOWRATE.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, self._name, self._id).inc()

        dp_phase_con = DataPackageController()
        with self._lock:
            dp_phase_con.controller_id=self._id
            dp_phase_con.controller_name=self._name
            dp_phase_con.mode=controller_mode_to_str(self._mode)
            dp_phase_con.workers=self._max_workers
            dp_phase_con.sequence_number=self._order_tracker.get_next_sequence_number()
        dp_phase_con.status = Status.RUNNING
        dp_phase_con.start_time=start_time

        # print(f"Starting {data_package.data} with sequence number {dp_phase_ex.sequence_number}")
        data_package.controllers.append(dp_phase_con)

        self._order_tracker.add_data(data_package, dp_phase_con)
        
        start_context = threading.current_thread().name

        def execute_phases() -> None:
            try:
                while True:
                    queue_data: Optional[QueueData] = None
                    with self._dp_queue_lock:
                        if len(self._dp_queue) == 0:
                            break

                        queue_data = self._dp_queue.popleft()

                    if queue_data is None:
                        continue

                    with self._lock:
                        CONTROLLER_INPUT_WAITING_COUNTER.labels(queue_data.data_package.pipeline_name, queue_data.data_package.pipeline_id, queue_data.data_package.pipeline_instance_id, self._name, self._id).dec()
                        CONTROLLER_PROCESSING_COUNT.labels(queue_data.data_package.pipeline_name, queue_data.data_package.pipeline_id, queue_data.data_package.pipeline_instance_id, self._name, self._id).inc()


                    # print(f"C{len(queue_data.data_package.controller)} Starting {queue_data.data_package.data} with sequence number {queue_data.dp_phase_con.sequence_number}")

                    start_context = queue_data.start_context
                    dp_phase_con = queue_data.dp_phase_con
                    data_package = queue_data.data_package
                    start_time = queue_data.start_time

                    # set the context of the thread
                    threading.current_thread().start_context = start_context # type: ignore
                    
                    try:
                        dp_phase_con.status = Status.RUNNING
                        waiting_time = time.time() - start_time
                        dp_phase_con.input_waiting_time = waiting_time
                        with self._lock:
                            CONTROLLER_INPUT_WAITING_TIME.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, self._name, self._id).observe(waiting_time)

                        temp_phases = []
                        with self._lock:
                            temp_phases = self._phases.copy()

                        for phase in temp_phases:
                            dpp = phase.execute(data_package, dp_phase_con)
                            if not dpp.status == Status.SUCCESS:
                                dp_phase_con.status = dpp.status
                                break

                        # print(f"Phase {self._name} finished {data_package.data} with sequence number {dp_phase_ex.sequence_number}: {id(self._order_tracker)}")

                    except Exception as e:
                        # print(exception_to_error(e))
                        dp_phase_con.status = Status.ERROR
                        data_package.errors.append(exception_to_error(e))

                    if dp_phase_con.status == Status.RUNNING:
                        dp_phase_con.status = Status.WAITING_OUTPUT
                    elif dp_phase_con.status == Status.EXIT:
                        dp_phase_con.status = Status.WAITING_OUTPUT_FOR_EXIT
                    elif dp_phase_con.status == Status.ERROR:
                        dp_phase_con.status = Status.WAITING_OUTPUT_FOR_ERROR
                    else:
                        try:
                            raise ValueError("Status not recognized.")
                        except ValueError as e:
                            dp_phase_con.status = Status.ERROR
                            data_package.errors.append(exception_to_error(e))

                    dp_phase_con.end_time = time.time() # This will set the end time temporarily (bad code). It will be overriden in the next block.
                    CONTROLLER_OUTPUT_WAITING_COUNTER.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, self._name, self._id).inc()
                    with self._order_tracker.instance_lock:
                        # print(f"C{len(queue_data.data_package.controller)} Finished {queue_data.data_package.data} with sequence number {queue_data.dp_phase_con.sequence_number}")
                        self._order_tracker.push_finished_data_package(dp_phase_con.sequence_number)
                        finished_data_packages, outdated_data_packages = self._order_tracker.pop_finished_data_packages(self._mode)
                        # print(f"C{len(queue_data.data_package.controller)} {len(finished_data_packages)}")
                        for (fdp, fdpc) in finished_data_packages:
                            
                            if fdpc.status == Status.WAITING_OUTPUT:
                                fdpc.status = Status.SUCCESS
                            elif fdpc.status == Status.WAITING_OUTPUT_FOR_EXIT:
                                fdpc.status = Status.EXIT
                            elif fdpc.status == Status.WAITING_OUTPUT_FOR_ERROR:
                                fdpc.status = Status.ERROR
                            else:
                                try:
                                    raise ValueError("Status not recognized.")
                                except ValueError as e:
                                    fdpc.status = Status.ERROR
                                    fdp.errors.append(exception_to_error(e))
                            
                            end_time = time.time()
                            total_time = end_time - fdp.start_time
                            output_waiting_time = end_time - fdpc.end_time # This is why the end time was temporarily set.
                            fdpc.output_waiting_time = output_waiting_time
                            fdpc.end_time = end_time # This is where the end time will be overriden. (bad code)
                            fdpc.total_time = total_time
                            CONTROLLER_OUTPUT_WAITING_TIME.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).observe(output_waiting_time)
                            CONTROLLER_OUTPUT_WAITING_COUNTER.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).dec()
                            
                            with self._lock:
                                if fdpc.status == Status.SUCCESS:
                                    CONTROLLER_OUTPUT_FLOWRATE.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).inc()
                                elif fdpc.status == Status.EXIT:
                                    CONTROLLER_EXIT_FLOWRATE.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).inc()
                                else:
                                    CONTROLLER_ERROR_FLOWRATE.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).inc()
                                CONTROLLER_PROCESSING_COUNT.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).dec()
                                CONTROLLER_TOTAL_TIME.labels(fdp.pipeline_name, fdp.pipeline_id, fdp.pipeline_instance_id, self._name, self._id).observe(total_time)
                            
                            callback(fdp, fdpc)
                            
                        for (odp, odpc) in outdated_data_packages:
                            if odpc.status == Status.WAITING_OUTPUT:
                                odpc.status = Status.OUTDATED
                            elif odpc.status == Status.WAITING_OUTPUT_FOR_EXIT:
                                odpc.status = Status.EXIT
                            elif odpc.status == Status.WAITING_OUTPUT_FOR_ERROR:
                                odpc.status = Status.ERROR
                            else:
                                try:
                                    raise ValueError("Status not recognized.")
                                except ValueError as e:
                                    odpc.status = Status.ERROR
                                    odp.errors.append(exception_to_error(e))
                            
                            end_time = time.time()
                            total_time = end_time - odp.start_time
                            output_waiting_time = end_time - odpc.end_time
                            
                            CONTROLLER_OUTDATED_FLOWRATE.labels(odp.pipeline_name, odp.pipeline_id, odp.pipeline_instance_id, self._name, self._id).inc()
                            
                            outdated_callback(odp)
                            
            except Exception as e:
                print(exception_to_error(e))

        popped_value: Optional[QueueData] = None
        with self._dp_queue_lock:
            if len(self._dp_queue) == self._dp_queue.maxlen:
                popped_value = self._dp_queue[0]

            # print(f"C{len(data_package.controller)} Put in Queue: {data_package.data}")
            with self._lock:
                CONTROLLER_INPUT_WAITING_COUNTER.labels(data_package.pipeline_name, data_package.pipeline_id, data_package.pipeline_instance_id, self._name, self._id).inc()
            
            dp_phase_con.status = Status.WAITING
            self._dp_queue.append(QueueData(start_context, dp_phase_con, data_package, start_time))

            if popped_value:
                with self._order_tracker.instance_lock:
                    self._order_tracker.remove_data(popped_value.dp_phase_con.sequence_number)
                    # print(f"Pop from Queue with E: {popped_value.data_package.data}")

            if self._executor._work_queue.qsize() < self._executor._max_workers:
                self._executor.submit(execute_phases)

        if popped_value:
            dp: DataPackage = popped_value.data_package
            popped_value.dp_phase_con.status = Status.OVERFLOW
            with self._lock:
                CONTROLLER_OVERFLOW_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, dp.pipeline_instance_id, self._name, self._id).inc()
                CONTROLLER_INPUT_WAITING_COUNTER.labels(dp.pipeline_name, dp.pipeline_id, dp.pipeline_instance_id, self._name, self._id).dec()
            overflow_callback(dp)
        


        
    def __deepcopy__(self, memo: Dict) -> 'PipelineController':
        copied_controller = PipelineController(
            name=self._name,
            phases=[phase.__deepcopy__(memo) for phase in self._phases],
            mode=self._mode,
            queue_size=self._queue_size,
            max_workers=self._max_workers,
        )
        copied_controller._id = self._id
        return copied_controller
    
    def get_id(self) -> str:
        with self._lock:
            return self._id

    def set_order_tracker(self, order_tracker: OrderTracker) -> None:
        with self._lock:
            self._order_tracker = order_tracker

class PipelineInstance:
    def __init__(self) -> None:
        self._id: str = f"PI-{uuid.uuid4()}"
        self._controller_queue: Dict[str, List[PipelineController]] = {}

        self._lock = threading.Lock()

    def execute(self, controllers: List[PipelineController], dp: DataPackage, callback: Callable[[DataPackage], None], exit_callback: Optional[Callable[[DataPackage], None]] = None, overflow_callback: Optional[Callable[[DataPackage], None]] = None, outdated_callback: Optional[Callable[[DataPackage], None]] = None, error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:      
        with self._lock:
            PIPELINE_INPUT_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
            PIPELINE_PROCESSING_COUNT.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
        
        with self._lock:
            dp.pipeline_instance_id = self._id

        self._controller_queue[dp.id] = controllers.copy()
        
        def end_dp(dp: DataPackage) -> None:
            del self._controller_queue[dp.id]
            end_time = time.time()
            dp.end_time = end_time
            total_time = end_time - dp.start_time
            dp.total_time = total_time
            with self._lock:
                PIPELINE_TOTAL_TIME.labels(dp.pipeline_name, dp.pipeline_id, self._id).observe(total_time)
                PIPELINE_PROCESSING_COUNT.labels(dp.pipeline_name, dp.pipeline_id, self._id).dec()
        
        def new_success_callback(dp: DataPackage) -> None:
            nonlocal callback
            end_dp(dp)
            dp.status = Status.SUCCESS
            with self._lock:
                PIPELINE_OUTPUT_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
            callback(dp)
        
        def new_exit_callback(dp: DataPackage) -> None:
            nonlocal exit_callback
            end_dp(dp)
            dp.status = Status.EXIT
            with self._lock:
                PIPELINE_EXIT_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
            if exit_callback:
                exit_callback(dp)
        
        def new_overflow_callback(dp: DataPackage) -> None:
            nonlocal overflow_callback
            end_dp(dp)
            dp.status = Status.OVERFLOW
            with self._lock:
                PIPELINE_OVERFLOW_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
            if overflow_callback:
                overflow_callback(dp)
                
        def new_outdated_callback(dp: DataPackage) -> None:
            nonlocal outdated_callback
            end_dp(dp)
            dp.status = Status.OUTDATED
            with self._lock:
                PIPELINE_OUTDATED_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
            if outdated_callback:
                outdated_callback(dp)
        
        def new_error_callback(dp: DataPackage) -> None:
            nonlocal error_callback
            end_dp(dp)
            dp.status = Status.ERROR
            with self._lock:
                PIPELINE_ERROR_FLOWRATE.labels(dp.pipeline_name, dp.pipeline_id, self._id).inc()
            if error_callback:
                error_callback(dp)

        def new_callback(dp: DataPackage, dpc: Optional[DataPackageController] = None) -> None:
            left_phases = []
            # if not dp.id in self._controller_queue:
            #     return
            for controller in self._controller_queue[dp.id]:
                left_phases.append(controller._name)

            # print(f"{dp.data} {len(dp.phases)}/{len(phases)}({len(self._phases_execution_queue[dp.id])}) {left_phases}")

            if dpc and not dpc.status == Status.SUCCESS:
                if dpc.status == Status.EXIT:
                    new_exit_callback(dp)
                elif dpc.status == Status.ERROR:
                    new_error_callback(dp)
                else:
                    try:
                        raise ValueError("Status not recognized.")
                    except ValueError as e:
                        dp.errors.append(exception_to_error(e))
                        new_error_callback(dp)
                return

            if len(self._controller_queue[dp.id]) > 0:
                controller = self._controller_queue[dp.id].pop(0)
                controller.execute(dp, new_callback, new_overflow_callback, new_outdated_callback)
                # print(f"Task {dp.data} submitted to {phase._name}. Remaining tasks: {len(phases_queue)}")
                return
            
            new_success_callback(dp)

        new_callback(dp)

    def get_id(self) -> str:
        with self._lock:
            return self._id

T = TypeVar('T')

class Pipeline(Generic[T]):
    def __init__(self, controllers_or_phases: Union[Sequence[Union[PipelineController, PipelinePhase]], None] = None, name: str = "") -> None:
        self._id: str = f"P-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._controllers: List[PipelineController] = []
        self._instances_controllers: Dict[str, List[PipelineController]] = {} # Each instance has its own copy of the controllers
        self._pipeline_instances: Dict[str, PipelineInstance] = {} # Dict of instances

        self._lock = threading.Lock()

        self.set_phases(controllers_or_phases)

    def get_id(self) -> str:
        with self._lock:
            return self._id
        
    def get_name(self) -> str:
        with self._lock:
            return self._name

    def set_phases(self, controllers_or_phases: Union[Sequence[Union[PipelineController, PipelinePhase]], None] = None) -> None:
        """
        This will set the phases for the pipeline and create a deepcopy for each phase and modules.
        """

        if controllers_or_phases:
            temp_controllers = []
            for c_or_p in controllers_or_phases:
                if isinstance(c_or_p, PipelinePhase):
                    phase_list = [c_or_p]
                    temp_controllers.append(PipelineController(self._name + "-Controller", phase_list))
                else:
                    temp_controllers.append(c_or_p)
        
            with self._lock:
                for c_or_p in temp_controllers:
                    self._controllers.append(c_or_p.__deepcopy__({}))

        # for each instance create a deepcopy of the phases
        with self._lock:
            for con in self._controllers:
                order_tracker = OrderTracker()
                for ex_id in self._pipeline_instances:
                    copy_con = con.__deepcopy__({})
                    copy_con.set_order_tracker(order_tracker)
                    self._instances_controllers[ex_id].append(copy_con)

    def register_instance(self) -> str:
        ex = PipelineInstance()
        with self._lock:
            self._pipeline_instances[ex.get_id()] = ex
            self._instances_controllers[ex.get_id()] = []
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
                del self._instances_controllers[ex_id]

    def execute(self, data: T, instance_id: str, callback: Callable[[DataPackage[T]], None], exit_callback: Optional[Callable[[DataPackage[T]], None]] = None, overflow_callback: Optional[Callable[[DataPackage[T]], None]] = None, outdated_callback: Optional[Callable[[DataPackage[T]], None]] = None, error_callback: Optional[Callable[[DataPackage[T]], None]] = None) -> DataPackage[T]:
        ex = self._get_instance(instance_id)
        if not ex:
            raise ValueError("Instance ID not found")
        
        temp_phases = []
        with self._lock:
            temp_phases = self._instances_controllers.get(instance_id, []).copy()

        # Put data into DataPackage
        dp = DataPackage[T]()
        with self._lock:
            dp.pipeline_id=self._id
            dp.pipeline_name=self._name
        dp.pipeline_instance_id=instance_id
        dp.data=data
        dp.start_time=time.time()

        dp.status = Status.RUNNING
        ex.execute(temp_phases, dp, callback, exit_callback, overflow_callback, outdated_callback, error_callback)
        return dp