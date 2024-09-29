# Stream Pipeline

![](img/small-logo.png)

## Overview
`stream_pipeline` is a modular pipeline designed to handle data streams, for eaxample audio streams. This project aims to provide a robust and flexible framework for processing streaming data through a series of modular and configurable components. Each step will be measured and can be used to optimize the pipeline. For a detailed description of the architecture, please refer to the [docs](https://juliankropp.github.io/stream_pipeline/)

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
3. Example: Create three files called `main.py`, `server_external_module.py` and `data.py` in the root of your project and add this example from this repository to the files.
- [`main.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/main.py)
- [`server_external_module.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/server_external_module.py)
- [`data.py`](https://github.com/JulianKropp/stream_pipeline/blob/main/data.py)
```bash
wget https://raw.githubusercontent.com/JulianKropp/stream_pipeline/main/server_external_module.py
wget https://raw.githubusercontent.com/JulianKropp/stream_pipeline/main/main.py
wget https://raw.githubusercontent.com/JulianKropp/stream_pipeline/main/data.py
```
4. Open two terminals and run the following commands to start the pipeline:
```bash
python3 server_external_module.py
```
```bash
python3 main.py
```


## Architecture
The pipeline is designed to be modular and flexible. Each module can be replaced with a custom implementation. For a deatiled description of the architecture, please refer to the [docs](https://juliankropp.github.io/stream_pipeline/)

# Dev

```bash
git clone https://github.com/JulianKropp/stream_pipeline
cd stream_pipeline
```

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


# License

[MIT License](LICENSE)