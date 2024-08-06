

from concurrent.futures import Future, ThreadPoolExecutor
import time

from stream_pipeline.error import exception_to_error
from .module_classes import Module
import threading
from typing import Any, Callable, Dict, List, Sequence, Union
from enum import Enum
import uuid
from prometheus_client import Gauge, Summary

from .data_package import DataPackage, DataPackageModule, DataPackagePhase, DataPackagePhaseController

PIPELINE_ERROR_COUNTER = Gauge('pipeline_error_counter', 'Number of errors in the pipeline', ['pipeline_name'])
PIPELINE_PROCESSING_COUNTER = Gauge('pipeline_processing_counter', 'Number of processes executing the pipline at the moment', ['pipeline_name'])
PIPELINE_PROCESSING_TIME = Summary('pipeline_processing_time', 'Time spent processing the pipeline', ['pipeline_name'])
PIPELINE_PROCESSING_TIME_WITHOUT_ERROR = Summary('pipeline_processing_time_without_error', 'Time spent processing the pipeline without error', ['pipeline_name'])
PIPELINE_WAITING_COUNTER = Gauge('pipeline_waiting_counter', 'Number of processes waiting for the pipline to be executed', ['pipeline_name'])
PIPELINE_WAITING_TIME = Summary('pipeline_waiting_time', 'Time spent waiting for the pipeline to be executed', ['pipeline_name'])



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
    def __init__(self, modules: List[Module], name: str = "") -> None:
        self._id: str = f"PP-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._modules: List[Module] = modules

        self._lock = threading.Lock()

    def execute(self, data_package: DataPackage, data_package_controller: DataPackagePhaseController) -> None:
        start_time = time.time()

        with self._lock:
            dp_phase = DataPackagePhase(
                running=True,
                start_time=start_time,
            )
        data_package_controller.phases.append(dp_phase)

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

    def pop_finished_data_packages(self, mode: ControllerMode) -> List[DataPackage]:
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
            finished_data_packages: List[DataPackage] = []

            if (mode == ControllerMode.NO_ORDER):
                for sequence_number, data_package in self._finished_data_packages.items():
                    finished_data_packages.append(data_package)
                self._finished_data_packages = {}
                    
            elif mode == ControllerMode.ORDER_BY_SEQUENCE or mode == ControllerMode.NOT_PARALLEL:
                current_sequence = self._last_finished_sequence_number + 1

                while current_sequence in self._finished_data_packages:
                    data_package = self._finished_data_packages.pop(current_sequence)
                    finished_data_packages.append(data_package)
                    self._last_finished_sequence_number = current_sequence
                    current_sequence = current_sequence + 1

            elif mode == ControllerMode.FIRST_WINS:
                # find each dp which has a bigger sequence number than the last finished sequence number
                last_sequence_number = self._last_finished_sequence_number + 1
                for sequence_number, data_package in self._finished_data_packages.items():
                    if sequence_number >= last_sequence_number:
                        finished_data_packages.append(data_package)
                        self._last_finished_sequence_number = sequence_number
                        last_sequence_number = sequence_number + 1

                # remove the data packages from the finished queue
                self._finished_data_packages.clear()
                
            return finished_data_packages

class PipelineController:
    def __init__(self, phases: List[PipelinePhase], name: str = "", max_workers: int = 1, mode: ControllerMode = ControllerMode.NOT_PARALLEL) -> None:
        self._id: str = f"PPE-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._phases: List[PipelinePhase] = phases
        self._mode: ControllerMode = mode

        if mode == ControllerMode.NOT_PARALLEL and max_workers > 1:
            max_workers = 1

        self._max_workers = max_workers

        self._executor = ThreadPoolExecutor(max_workers=max_workers) if max_workers > 0 else None
        self._active_futures: Dict[str, Future] = {}

        self._order_tracker = OrderTracker()

        self._lock = threading.Lock()

    def execute(self, instance_lock: threading.Lock, data_package: DataPackage, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:
        start_time = time.time()

        dp_phase_con = DataPackagePhaseController(
            mode=controller_mode_to_str(self._mode),
            workers=self._max_workers,
            sequence_number=self._order_tracker.get_next_sequence_number(),
            running=True,
            start_time=start_time,
        )

        # print(f"Starting {data_package.data} with sequence number {dp_phase_ex.sequence_number}")
        data_package.controller.append(dp_phase_con)
        
        self._order_tracker.add_data(data_package)
        
        start_context = threading.current_thread().name

        def execute_phases() -> None:
            nonlocal start_context, dp_phase_con, data_package, start_time, instance_lock
            try:
                start_processing_time = time.time()
                dp_phase_con.waiting_time = time.time() - start_time

                temp_phases = []
                with self._lock:
                    temp_phases = self._phases.copy()

                for phase in temp_phases:
                    phase.execute(data_package, dp_phase_con)
                    if not data_package.success:
                        break

                # print(f"Phase {self._name} finished {data_package.data} with sequence number {dp_phase_ex.sequence_number}: {id(self._order_tracker)}")

            except Exception as e:
                data_package.success = False
                data_package.errors.append(exception_to_error(e))


            dp_phase_con.end_time = time.time()
            dp_phase_con.processing_time = dp_phase_con.end_time - start_processing_time
            dp_phase_con.total_time = dp_phase_con.end_time - dp_phase_con.start_time
            dp_phase_con.running = False

            with instance_lock:
                self._order_tracker.push_finished_data_package(dp_phase_con.sequence_number)
                finished_data_packages = self._order_tracker.pop_finished_data_packages(self._mode)
                for finished_data_package in finished_data_packages:
                    if finished_data_package.success:
                        callback(finished_data_package)
                    else:
                        if error_callback:
                            error_callback(finished_data_package)

        
        if self._executor is None:
            execute_phases()
            return

        eid = f"{self.get_id()}-{dp_phase_con.sequence_number}"
        future = self._executor.submit(execute_phases)
        if self._mode == ControllerMode.FIRST_WINS:
            self._active_futures[eid] = future
        


        
    def __deepcopy__(self, memo: Dict) -> 'PipelineController':
        copied_controller = PipelineController(
            name=self._name,
            phases=[phase.__deepcopy__(memo) for phase in self._phases],
            mode=self._mode,
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
    def __init__(self, name: str = "") -> None:
        self._id: str = f"PI-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._controller_queue: Dict[str, List[PipelineController]] = {}

        self._lock = threading.Lock()
        self._execution_lock = threading.Lock()

    def execute(self, controllers: List[PipelineController], dp: DataPackage, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:      
        dp.pipeline_instance_id = self._id

        self._controller_queue[dp.id] = controllers.copy()

        def new_callback(dp: DataPackage) -> None:
            nonlocal callback, error_callback
            left_phases = []
            for controller in self._controller_queue[dp.id]:
                left_phases.append(controller._name)

            # print(f"{dp.data} {len(dp.phases)}/{len(phases)}({len(self._phases_execution_queue[dp.id])}) {left_phases}")

            if dp.success:
                if len(self._controller_queue[dp.id]) > 0:
                    controller = self._controller_queue[dp.id].pop(0)
                    controller.execute(self._execution_lock, dp, new_callback, error_callback)
                    # print(f"Task {dp.data} submitted to {phase._name}. Remaining tasks: {len(phases_queue)}")
                    return
            
            del self._controller_queue[dp.id]

            dp.end_time = time.time()
            dp.total_time = dp.end_time - dp.start_time

            def calculate_total_waiting_time(module: DataPackageModule) -> float:
                total_waiting_time = module.waiting_time
                for sub_module in module.sub_modules:
                    total_waiting_time += calculate_total_waiting_time(sub_module)
                return total_waiting_time

            # calculate waiting time
            temp_waiting = 0.0
            for dp_controller in dp.controller:
                temp_waiting += dp_controller.waiting_time
                for ph in dp_controller.phases:
                    for module in ph.modules:
                        temp_waiting += calculate_total_waiting_time(module)
            
            dp.total_waiting_time = temp_waiting
            dp.total_processing_time = sum([dp_controller.processing_time for dp_controller in dp.controller]) - dp.total_waiting_time

            dp.running = False

            if dp.success:
                callback(dp)
            else:
                if error_callback:
                    error_callback(dp)

        new_callback(dp)

    def get_id(self) -> str:
        with self._lock:
            return self._id
        
    def get_name(self) -> str:
        with self._lock:
            return self._name

class Pipeline:
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
                    temp_controllers.append(PipelineController(phase_list))
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
        ex = PipelineInstance(name=f"")
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

    def execute(self, data: Any, instance_id: str, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> DataPackage:
        ex = self._get_instance(instance_id)
        if not ex:
            raise ValueError("Instance ID not found")
        
        temp_phases = []
        with self._lock:
            temp_phases = self._instances_controllers.get(instance_id, []).copy()

        # Put data into DataPackage
        dp = DataPackage(
            pipeline_id=self._id,
            pipeline_instance_id=instance_id,
            data=data,
            start_time=time.time(),
        )

        dp.running = True
        ex.execute(temp_phases, dp, callback, error_callback)
        return dp