# Stream Pipeline

![](img/small-logo.png)

## Overview
`stream_pipeline` is a scalable microservice pipeline designed to handle data streams, for eaxample audio streams. This project aims to provide a robust and flexible framework for processing streaming data through a series of modular and configurable components.

## How to use this package
1. Create a new file called requirements.txt in the root of your project and add the following line:
```
git+https://github.com/JulianKropp/stream_pipeline
mypy
```
2. Install the package by running the following command:
```bash
pip3 install -r requirements.txt
```
3. Example: Create two files called [`main.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/main.py) and [`server_external_module.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/server_external_module.py) in the root of your project and add this example from this repository to the files.
- [`main.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/main.py)
- [`server_external_module.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/server_external_module.py)
```bash
wget https://raw.githubusercontent.com/JulianKropp/stream_pipeline/main/server_external_module.py
wget https://raw.githubusercontent.com/JulianKropp/stream_pipeline/main/main.py
```
4. Open two terminals and run the following commands to start the pipeline:
```bash
python3 server_external_module.py
```
```bash
python3 main.py
```


## Architecture
The architecture of `stream_pipeline` is built around a series of controllers and phases, each comprising multiple modules that process the data sequentially or in parallel, depending on the configuration. Below is a high-level description of the architecture:

### Controllers
Controllers manage the flow of data through the pipeline. Each controller can operate in different modes:
- **NOT_PARALLEL**: Processes data sequentially through phases.
- **NO_ORDER**: Processes data in parallel without any specific order.
- **ORDER_BY_SEQUENCE**: Processes data in parallel but maintains a sequence order.
- **FIRST_WINS**: Processes data in parallel and prioritizes the first completed process.

### Phases
Each controller consists of multiple phases. A phase represents a stage in the data processing pipeline. Within each phase, data passes through several modules.

### Modules
Modules are the building blocks of the pipeline. They perform specific tasks on the data stream. There are several default types of modules you can use to create your own modules:
- **ExecutionModule**: Executes code to modify or process the data.
- **ConditionModule**: Evaluates conditions and routes the data based on the result.
- **CombinationModule**: Combines multiple modules into one.
- **ExternalModule**: Moves the execution of an module to an external server.

# Detailed Architecture Description
## Pipeline
```mermaid
stateDiagram-v2
    [*] --> Pipeline: Start

    state Pipeline {

        [*] --> Controller0_NOT_PARALLEL

        state Controller0_NOT_PARALLEL {

            [*] --> Phase0_C0

            state Phase0_C0 {
                [*] --> Module0_P0_C0: Start Phase0
                Module0_P0_C0 --> Module1_P0_C0
                Module1_P0_C0 --> Module...n_P0_C0
                Module...n_P0_C0 --> [*]: End Phase0
            }

            Phase0_C0 --> Phase1_C0

            state Phase1_C0 {
                [*] --> Module0_P1_C0: Start Phase1
                Module0_P1_C0 --> Module1_P1_C0
                Module1_P1_C0 --> Module...n_P1_C0
                Module...n_P1_C0 --> [*]: End Phase1
            }

            Phase1_C0 --> Phase...n_C0

            state Phase...n_C0 {
                [*] --> Module0_P...n_C0: Start Phase...n
                Module0_P...n_C0 --> Module1_P...n_C0
                Module1_P...n_C0 --> Module...n_P...n_C0
                Module...n_P...n_C0 --> [*]: End Phase...n
            }

            Phase...n_C0 --> [*]
        }

        Controller0_NOT_PARALLEL --> Controller1_NO_ORDER

        state Controller1_NO_ORDER {

            state fork_state_C0 <<fork>>
            [*] --> fork_state_C0
            fork_state_C0 --> Phase0_C1: 0
            fork_state_C0 --> Phase0_C1: 1
            fork_state_C0 --> Phase0_C1: ...n

            state Phase0_C1 {
                [*] --> Module0_P0_C1: Start Phase0
                Module0_P0_C1 --> Module1_P0_C1
                Module1_P0_C1 --> Module...n_P0_C1
                Module...n_P0_C1 --> [*]: End Phase0
            }

            Phase0_C1 --> Phase1_C1: 0
            Phase0_C1 --> Phase1_C1: 1
            Phase0_C1 --> Phase1_C1: ...n

            state Phase1_C1 {
                [*] --> Module0_P1_C1: Start Phase1
                Module0_P1_C1 --> Module1_P1_C1
                Module1_P1_C1 --> Module...n_P1_C1
                Module...n_P1_C1 --> [*]: End Phase1
            }

            Phase1_C1 --> Phase...n_C1: 0
            Phase1_C1 --> Phase...n_C1: 1
            Phase1_C1 --> Phase...n_C1: ...n

            state Phase...n_C1 {
                [*] --> Module0_P...n_C1: Start Phase...n
                Module0_P...n_C1 --> Module1_P...n_C1
                Module1_P...n_C1 --> Module...n_P...n_C1
                Module...n_P...n_C1 --> [*]: End Phase...n
            }

            state join_state_C0 <<join>>
            Phase...n_C1 --> join_state_C0: 0
            Phase...n_C1 --> join_state_C0: 1
            Phase...n_C1 --> join_state_C0: ...n
            join_state_C0 --> [*]
        }

        Controller1_NO_ORDER --> Controller2_ORDER_BY_SEQUENCE

        state Controller2_ORDER_BY_SEQUENCE {

            [*] --> Add_Sequence_Number_C2

            state fork_state_C1 <<fork>>
            Add_Sequence_Number_C2 --> fork_state_C1
            fork_state_C1 --> Phase0_C2: 0
            fork_state_C1 --> Phase0_C2: 1
            fork_state_C1 --> Phase0_C2: ...n

            state Phase0_C2 {
                [*] --> Module0_P0_C2: Start Phase0
                Module0_P0_C2 --> Module1_P0_C2
                Module1_P0_C2 --> Module...n_P0_C2
                Module...n_P0_C2 --> [*]: End Phase0
            }

            Phase0_C2 --> Phase1_C2: 0
            Phase0_C2 --> Phase1_C2: 1
            Phase0_C2 --> Phase1_C2: ...n

            state Phase1_C2 {
                [*] --> Module0_P1_C2: Start Phase1
                Module0_P1_C2 --> Module1_P1_C2
                Module1_P1_C2 --> Module...n_P1_C2
                Module...n_P1_C2 --> [*]: End Phase1
            }

            Phase1_C2 --> Phase...n_C2: 0
            Phase1_C2 --> Phase...n_C2: 1
            Phase1_C2 --> Phase...n_C2: ...n

            state Phase...n_C2 {
                [*] --> Module0_P...n_C2: Start Phase...n
                Module0_P...n_C2 --> Module1_P...n_C2
                Module1_P...n_C2 --> Module...n_P...n_C2
                Module...n_P...n_C2 --> [*]: End Phase...n
            }

            state join_state_C1 <<join>>
            Phase...n_C2 --> join_state_C1: 0
            Phase...n_C2 --> join_state_C1: 1
            Phase...n_C2 --> join_state_C1: ...n
            join_state_C1 --> Order_By_Sequence_Number_C2
            Order_By_Sequence_Number_C2 --> [*]
        }

        Controller2_ORDER_BY_SEQUENCE --> Controller3_FIRST_WINS

        state Controller3_FIRST_WINS {

            [*] --> Add_Sequence_Number_C3

            state fork_state_C3 <<fork>>
            Add_Sequence_Number_C3 --> fork_state_C3
            fork_state_C3 --> Phase0_C3: 0
            fork_state_C3 --> Phase0_C3: 1
            fork_state_C3 --> Phase0_C3: ...n

            state Phase0_C3 {
                [*] --> Module0_P0_C3: Start Phase0
                Module0_P0_C3 --> Module1_P0_C3
                Module1_P0_C3 --> Module...n_P0_C3
                Module...n_P0_C3 --> [*]: End Phase0
            }

            Phase0_C3 --> Phase1_C3: 0
            Phase0_C3 --> Phase1_C3: 1
            Phase0_C3 --> Phase1_C3: ...n

            state Phase1_C3 {
                [*] --> Module0_P1_C3: Start Phase1
                Module0_P1_C3 --> Module1_P1_C3
                Module1_P1_C3 --> Module...n_P1_C3
                Module...n_P1_C3 --> [*]: End Phase1
            }

            Phase1_C3 --> Phase...n_C3: 0
            Phase1_C3 --> Phase...n_C3: 1
            Phase1_C3 --> Phase...n_C3: ...n

            state Phase...n_C3 {
                [*] --> Module0_P...n_C3: Start Phase...n
                Module0_P...n_C3 --> Module1_P...n_C3
                Module1_P...n_C3 --> Module...n_P...n_C3
                Module...n_P...n_C3 --> [*]: End Phase...n
            }

            state join_state_C2 <<join>>
            Phase...n_C3 --> join_state_C2: 0
            Phase...n_C3 --> join_state_C2: 1
            Phase...n_C3 --> join_state_C2: ...n
            join_state_C2 --> Remove_Older_Then_Last_Sequence_Number_C3

            Remove_Older_Then_Last_Sequence_Number_C3 --> [*]
        }

    }

    Pipeline --> [*]: End


```

## Modules
### ExecutionModule
Executes code to modify or process the data.

```mermaid
stateDiagram-v2
    [*] --> ExecutionModule
    state ExecutionModule {
        [*] --> ExecuteCode
        ExecuteCode --> [*]
    }
    ExecutionModule --> [*]
```

### ConditionModule
Evaluates conditions and routes the data based on the result.

```mermaid
stateDiagram-v2
    [*] --> ConditionModule
    state ConditionModule {
        state if_state <<choice>>
        [*] --> ConditionCode
        ConditionCode --> if_state
        if_state --> TrueModule: If condition is true
        if_state --> FalseModule : If condition is false
        TrueModule --> [*]
        FalseModule --> [*]
    }
    ConditionModule --> [*]
```

### CombinationModule
Combines multiple modules into one.

```mermaid
stateDiagram-v2
    [*] --> CombinationModule
    state CombinationModule {
        [*] --> Module0: Start Processing
        state Module0 {
            [*] --> ExecuteCode0
            ExecuteCode0 --> [*]
        }
        Module0 --> Module1
        state Module1 {
            [*] --> ExecuteCode1
            ExecuteCode1 --> [*]
        }
        Module1 --> Module...n
        state Module...n {
            [*] --> ExecuteCode...n
            ExecuteCode...n --> [*]
        }
        Module...n --> [*]
    }
    CombinationModule --> [*]
```

### ExternalModule
Moves the execution of a module to an external server.

```mermaid
stateDiagram-v2
    state Server0 {
        state Pipeline0 {
            state Controller0 {
                state ExternalModule {
                    [*] --> StartExternalModule
                    EndExternalModule --> [*]
                }
            }
        }
    }
    state Server1 {
        Receiver --> Module
        Module --> Sender
    }
    StartExternalModule --> Receiver
    Sender --> EndExternalModule
```

# Future Plans

## Scaling the Pipeline
The pipeline is designed to be scalable, allowing for the distribution of data processing tasks across multiple instances or nodes. This is managed by a Lookaside Load Balancer, which distributes incoming requests to different pipelines based on the current load and availability.

### Lookaside Load Balancer
The load balancer ensures efficient distribution and management of pipelines and modules, providing a scalable solution to handle varying loads of data streams.

```mermaid
flowchart TD
    subgraph Clients
        direction RL
        ClientA[Client A]
        ClientB[Client B]
        ClientC[Client C]
        ClientD[Client D]
        ClientE[Client E]
        ClientF[Client F]
        ClientG[Client G]
        ClientH[Client H]
        ClientI[Client I]
    end
    ClientA -->|Request| PipelineA
    ClientB -->|Request| PipelineA
    ClientC -->|Request| PipelineB
    ClientD -->|Request| PipelineC
    ClientE -->|Request| PipelineD
    ClientF -->|Request| PipelineD
    ClientG -->|Request| PipelineD
    ClientH -->|Request| PipelineE
    ClientI -->|Request| PipelineF

    subgraph LoadBalancer
        direction LR
        LLBM[Lookaside Load Balancer Module]
        LLBP[Lookaside Load Balancer Pipeline]
    end

    Clients --> | Request a Pipeline | LLBP
    LLBM --> |Manage Pipelines| Pipelines
    Pipelines --> | Sends Status | LoadBalancer
    Modules -->|Sends Status| LLBM

    subgraph Modules
        direction RL
        ExternalModuleA[External Module A]
        ExternalModuleB[External Module B]
        ExternalModuleC[External Module C]
        ExternalModuleD[External Module D]
    end

    subgraph Pipelines
        direction RL
        PipelineA -->|Request| ExternalModuleA
        PipelineB -->|Request| ExternalModuleA
        PipelineC -->|Request| ExternalModuleB
        PipelineD -->|Request| ExternalModuleC
        PipelineE -->|Request| ExternalModuleD
        PipelineF -->|Request| ExternalModuleD
    end
```

# Dev
## Install dependencies to work
```
pip3 install -r requirements.txt
pip3 install grpcio-tools mypy-protobuf mypy
```

## Check for type errors
```
pip3 install mypy
mypy --check-untyped-defs --disallow-untyped-defs main.py
mypy --check-untyped-defs --disallow-untyped-defs server_external_module.py
```

## After changing something in proto file
Generate proto files:
```
pip3 install grpcio-tools mypy-protobuf mypy
cd stream_pipeline && python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --mypy_out=. --mypy_grpc_out=. data.proto && cd ..
```
There will be an import error:
error:
```json
{
    "message": "No module named 'data_pb2'",
    "id": "Error-e22a2f0e-bde3-4ae4-ac34-d77388b17a9e",
    "type": "ModuleNotFoundError",
    "traceback": [
        "Traceback (most recent call last):",
        "/home/user/stream_pipeline/main.py:171",
        "/home/user/stream_pipeline/main.py:11",
        "/home/user/stream_pipeline/stream_pipeline/module_classes.py:12",
        "/home/user/stream_pipeline/stream_pipeline/data_pb2_grpc.py:6",
        "ModuleNotFoundError: No module named 'data_pb2'"
    ],
    "thread": "MainThread",
    "start_context": "N/A",
    "thread_id": 140072941379584,
    "is_daemon": false
}
```
Fix: Change `import data_pb2 as data__pb2` to `from . import data_pb2 as data__pb2` in `stream_pipeline/data_pb2_grpc.py:6`


