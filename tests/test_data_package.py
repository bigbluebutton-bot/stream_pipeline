import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import unittest
from unittest.mock import MagicMock
from src.data_package import DataPackageModule, DataPackage
from src.error import Error
from src import data_pb2
import pickle


class TestDataPackageModule(unittest.TestCase):

    def setUp(self):
        self.module = DataPackageModule(
            id="test_module",
            running=True,
            start_time=1.0,
            end_time=2.0,
            waiting_time=0.5,
            processing_time=0.7,
            total_time=1.2,
            sub_modules=[],
            success=True,
            error=None
        )

    def test_to_grpc(self):
        grpc_module = self.module.to_grpc()
        self.assertEqual(grpc_module.module_id, self.module.id)
        self.assertEqual(grpc_module.running, self.module.running)
        self.assertEqual(grpc_module.start_time, self.module.start_time)
        self.assertEqual(grpc_module.end_time, self.module.end_time)
        self.assertEqual(grpc_module.waiting_time, self.module.waiting_time)
        self.assertEqual(grpc_module.processing_time, self.module.processing_time)
        self.assertEqual(grpc_module.total_time, self.module.total_time)
        self.assertEqual(grpc_module.success, self.module.success)

    def test_set_from_grpc(self):
        grpc_module = data_pb2.DataPackageModule(
            module_id="test_module",
            running=True,
            start_time=1.0,
            end_time=2.0,
            waiting_time=0.5,
            processing_time=0.7,
            total_time=1.2,
            success=True
        )
        self.module.set_from_grpc(grpc_module)
        self.assertEqual(self.module.id, grpc_module.module_id)
        self.assertEqual(self.module.running, grpc_module.running)
        self.assertEqual(self.module.start_time, grpc_module.start_time)
        self.assertEqual(self.module.end_time, grpc_module.end_time)
        self.assertEqual(self.module.waiting_time, grpc_module.waiting_time)
        self.assertEqual(self.module.processing_time, grpc_module.processing_time)
        self.assertEqual(self.module.total_time, grpc_module.total_time)
        self.assertEqual(self.module.success, grpc_module.success)


class TestDataPackage(unittest.TestCase):

    def setUp(self):
        self.package = DataPackage(
            pipeline_id="pipeline_1",
            pipeline_instance_id="executor_1",
            sequence_number=1,
            modules=[],
            data={"key": "value"},
            running=True,
            success=True,
            message="All good",
            errors=[]
        )

    def test_to_grpc(self):
        grpc_package = self.package.to_grpc()
        self.assertEqual(grpc_package.pipeline_id, self.package.pipeline_id)
        self.assertEqual(grpc_package.pipeline_executer_id, self.package.pipeline_instance_id)
        self.assertEqual(grpc_package.sequence_number, self.package.sequence_number)
        self.assertEqual(grpc_package.running, self.package.running)
        self.assertEqual(grpc_package.success, self.package.success)
        self.assertEqual(grpc_package.message, self.package.message)
        self.assertEqual(pickle.loads(grpc_package.data), self.package.data)

    def test_set_from_grpc(self):
        grpc_package = data_pb2.DataPackage(
            pipeline_id="pipeline_1",
            pipeline_executer_id="executor_1",
            sequence_number=1,
            data=pickle.dumps({"key": "value"}),
            running=True,
            success=True,
            message="All good"
        )
        self.package.set_from_grpc(grpc_package)
        self.assertEqual(self.package.pipeline_id, grpc_package.pipeline_id)
        self.assertEqual(self.package.pipeline_instance_id, grpc_package.pipeline_executer_id)
        self.assertEqual(self.package.sequence_number, grpc_package.sequence_number)
        self.assertEqual(self.package.running, grpc_package.running)
        self.assertEqual(self.package.success, grpc_package.success)
        self.assertEqual(self.package.message, grpc_package.message)
        self.assertEqual(self.package.data, pickle.loads(grpc_package.data))


class TestError(unittest.TestCase):

    def setUp(self):
        self.error = Error(
            type="ValueError",
            message="An error occurred",
            traceback=["traceback line 1", "traceback line 2"],
            thread="MainThread",
            start_context="start_context",
            thread_id=1234,
            is_daemon=False,
            local_vars={"var1": "value1"},
            global_vars={"var2": "value2"},
            environment_vars={"var3": "value3"},
            module_versions={"module1": "1.0.0"}
        )

    def test_to_grpc(self):
        grpc_error = self.error.to_grpc()
        self.assertEqual(grpc_error.type, self.error.type)
        self.assertEqual(grpc_error.message, self.error.message)
        self.assertEqual(grpc_error.traceback, self.error.traceback)
        self.assertEqual(grpc_error.thread, self.error.thread)
        self.assertEqual(grpc_error.start_context, self.error.start_context)
        self.assertEqual(grpc_error.thread_id, self.error.thread_id)
        self.assertEqual(grpc_error.is_daemon, self.error.is_daemon)
        self.assertEqual(grpc_error.local_vars, self.error.local_vars)
        self.assertEqual(grpc_error.global_vars, self.error.global_vars)
        self.assertEqual(grpc_error.environment_vars, self.error.environment_vars)
        self.assertEqual(grpc_error.module_versions, self.error.module_versions)

    def test_set_from_grpc(self):
        grpc_error = data_pb2.Error(
            type="ValueError",
            message="An error occurred",
            traceback=["traceback line 1", "traceback line 2"],
            thread="MainThread",
            start_context="start_context",
            thread_id=1234,
            is_daemon=False,
            local_vars={"var1": "value1"},
            global_vars={"var2": "value2"},
            environment_vars={"var3": "value3"},
            module_versions={"module1": "1.0.0"}
        )
        self.error.set_from_grpc(grpc_error)
        self.assertEqual(self.error.type, grpc_error.type)
        self.assertEqual(self.error.message, grpc_error.message)
        self.assertEqual(self.error.traceback, list(grpc_error.traceback))
        self.assertEqual(self.error.thread, grpc_error.thread)
        self.assertEqual(self.error.start_context, grpc_error.start_context)
        self.assertEqual(self.error.thread_id, grpc_error.thread_id)
        self.assertEqual(self.error.is_daemon, grpc_error.is_daemon)
        self.assertEqual(self.error.local_vars, dict(grpc_error.local_vars))
        self.assertEqual(self.error.global_vars, dict(grpc_error.global_vars))
        self.assertEqual(self.error.environment_vars, dict(grpc_error.environment_vars))
        self.assertEqual(self.error.module_versions, dict(grpc_error.module_versions))


class TestDataPackageModuleConversion(unittest.TestCase):

    def setUp(self):
        self.module = DataPackageModule(
            id="test_module",
            running=True,
            start_time=1.0,
            end_time=2.0,
            waiting_time=0.5,
            processing_time=0.7,
            total_time=1.2,
            sub_modules=[
                DataPackageModule(id="sub_module_1"),
                DataPackageModule(id="sub_module_2")
            ],
            success=True,
            error=None
        )

    def test_conversion(self):
        grpc_module = self.module.to_grpc()
        new_module = DataPackageModule()
        new_module.set_from_grpc(grpc_module)

        self.assertEqual(new_module.id, self.module.id)
        self.assertEqual(new_module.running, self.module.running)
        self.assertEqual(new_module.start_time, self.module.start_time)
        self.assertEqual(new_module.end_time, self.module.end_time)
        self.assertEqual(new_module.waiting_time, self.module.waiting_time)
        self.assertEqual(new_module.processing_time, self.module.processing_time)
        self.assertEqual(new_module.total_time, self.module.total_time)
        self.assertEqual(new_module.success, self.module.success)
        self.assertEqual(len(new_module.sub_modules), len(self.module.sub_modules))
        for new_sub, old_sub in zip(new_module.sub_modules, self.module.sub_modules):
            self.assertEqual(new_sub.id, old_sub.id)
        self.assertEqual(new_module.error, self.module.error)

class TestDataPackageConversion(unittest.TestCase):

    def setUp(self):
        self.package = DataPackage(
            pipeline_id="pipeline_1",
            pipeline_instance_id="executor_1",
            sequence_number=1,
            modules=[
                DataPackageModule(id="module_1"),
                DataPackageModule(id="module_2")
            ],
            data={"key": "value"},
            running=True,
            success=True,
            message="All good",
            errors=[
                Error(type="ValueError", message="An error occurred")
            ]
        )

    def test_conversion(self):
        grpc_package = self.package.to_grpc()
        new_package = DataPackage()
        new_package.set_from_grpc(grpc_package)

        self.assertEqual(new_package.pipeline_id, self.package.pipeline_id)
        self.assertEqual(new_package.pipeline_instance_id, self.package.pipeline_instance_id)
        self.assertEqual(new_package.sequence_number, self.package.sequence_number)
        self.assertEqual(new_package.running, self.package.running)
        self.assertEqual(new_package.success, self.package.success)
        self.assertEqual(new_package.message, self.package.message)
        self.assertEqual(new_package.data, self.package.data)
        self.assertEqual(len(new_package.modules), len(self.package.modules))
        for new_mod, old_mod in zip(new_package.modules, self.package.modules):
            self.assertEqual(new_mod.module_id, old_mod.module_id)
        self.assertEqual(len(new_package.errors), len(self.package.errors))
        for new_err, old_err in zip(new_package.errors, self.package.errors):
            self.assertEqual(new_err.type, old_err.type)
            self.assertEqual(new_err.message, old_err.message)

class TestErrorConversion(unittest.TestCase):

    def setUp(self):
        self.error = Error(
            type="ValueError",
            message="An error occurred",
            traceback=["traceback line 1", "traceback line 2"],
            thread="MainThread",
            start_context="start_context",
            thread_id=1234,
            is_daemon=False,
            local_vars={"var1": "value1"},
            global_vars={"var2": "value2"},
            environment_vars={"var3": "value3"},
            module_versions={"module1": "1.0.0"}
        )

    def test_conversion(self):
        grpc_error = self.error.to_grpc()
        new_error = Error()
        new_error.set_from_grpc(grpc_error)

        self.assertEqual(new_error.type, self.error.type)
        self.assertEqual(new_error.message, self.error.message)
        self.assertEqual(new_error.traceback, self.error.traceback)
        self.assertEqual(new_error.thread, self.error.thread)
        self.assertEqual(new_error.start_context, self.error.start_context)
        self.assertEqual(new_error.thread_id, self.error.thread_id)
        self.assertEqual(new_error.is_daemon, self.error.is_daemon)
        self.assertEqual(new_error.local_vars, self.error.local_vars)
        self.assertEqual(new_error.global_vars, self.error.global_vars)
        self.assertEqual(new_error.environment_vars, self.error.environment_vars)
        self.assertEqual(new_error.module_versions, self.error.module_versions)

if __name__ == '__main__':
    unittest.main()
