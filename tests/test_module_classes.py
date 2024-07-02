import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import unittest
from module_classes import Module, ExecutionModule, ConditionModule, CombinationModule
from processing import Processing, ProcessingManager

class SimpleExecutionModule(ExecutionModule):
    def execute(self, data):
        return True, "Simple Execution Successful", data + 1

class AlwaysTrueConditionModule(ConditionModule):
    def condition(self, data):
        return data % 2 == 0  # Returns True if data is even, otherwise False


class TestModules(unittest.TestCase):
    def setUp(self):
        self.simple_module = SimpleExecutionModule()
        self.true_condition_module = AlwaysTrueConditionModule(self.simple_module, self.simple_module)
        self.combination_module = CombinationModule([self.simple_module, self.simple_module])
        self.processing_manager = ProcessingManager([self.simple_module], [self.simple_module], [self.simple_module])

    def test_simple_execution_module(self):
        result, message, data = self.simple_module.execute(1)
        self.assertTrue(result)
        self.assertEqual(message, "Simple Execution Successful")
        self.assertEqual(data, 2)

    def test_condition_module_true_branch(self):
        result, message, data = self.true_condition_module.execute(2)
        self.assertTrue(result)
        self.assertEqual(message, "Simple Execution Successful")
        self.assertEqual(data, 3)

    def test_condition_module_false_branch(self):
        result, message, data = self.true_condition_module.execute(1)
        self.assertTrue(result)
        self.assertEqual(message, "Simple Execution Successful")
        self.assertEqual(data, 2)

    def test_combination_module(self):
        result, message, data = self.combination_module.execute(1)
        self.assertTrue(result)
        self.assertEqual(message, "")
        self.assertEqual(data, 3)

    def test_processing(self):
        processing = Processing([self.simple_module, self.simple_module])
        result, message, data = processing.execute(1)
        self.assertTrue(result)
        self.assertEqual(message, "Processing succeeded")
        self.assertEqual(data, 3)

    def test_processing_manager(self):
        result, message, data = self.processing_manager.run(1)
        self.assertTrue(result)
        self.assertEqual(message, "All processing succeeded")
        self.assertEqual(data, 4)

    def test_processing_manager_pre_failure(self):
        class FailingModule(Module):
            def execute(self, data):
                return False, "ERROR", data

        processing_manager = ProcessingManager([FailingModule()], [self.simple_module], [self.simple_module])
        result, message, data = processing_manager.run(1)
        self.assertFalse(result)
        self.assertEqual(message, "Pre-processing failed: Module 0 (FailingModule) failed: ERROR")
        self.assertEqual(data, 1)

    def test_processing_manager_main_failure(self):
        class FailingModule(Module):
            def execute(self, data):
                return False, "ERROR", data

        processing_manager = ProcessingManager([self.simple_module], [FailingModule()], [self.simple_module])
        result, message, data = processing_manager.run(1)
        self.assertFalse(result)
        self.assertEqual(message, "Main processing failed: Module 0 (FailingModule) failed: ERROR")
        self.assertEqual(data, 2)

    def test_processing_manager_post_failure(self):
        class FailingModule(Module):
            def execute(self, data):
                return False, "ERROR", data

        processing_manager = ProcessingManager([self.simple_module], [self.simple_module], [FailingModule()])
        result, message, data = processing_manager.run(1)
        self.assertFalse(result)
        self.assertEqual(message, "Post-processing failed: Module 0 (FailingModule) failed: ERROR")
        self.assertEqual(data, 3)

if __name__ == '__main__':
    unittest.main()
