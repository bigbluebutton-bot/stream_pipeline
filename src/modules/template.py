from module_classes import ExecutionModule, ConditionModule, CombinationModule, ModuleOptions

class TemplateModule(ExecutionModule):
    def __init__(self, options=ModuleOptions()):
        super().__init__(options)
        self.name = "Template"
        self.description = "This is a template module. It does nothing."
        self.options = ModuleOptions()

    def execute(self, data):
        return True, "Template module executed", data

class TemplateConditionModule(ConditionModule):
    def __init__(self, true_module, false_module, options=ModuleOptions()):
        super().__init__(true_module, false_module, options)
        self.name = "TemplateCondition"
        self.description = "This is a template condition module. It always returns true."
        self.options = ModuleOptions()

    def condition(self, data):
        return True