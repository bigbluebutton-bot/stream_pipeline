# Creating a data type
class Data:
    def __init__(self, key: str, condition: bool) -> None:
        self.key = key
        self.condition = condition
        self.status = "unknown"
        
    def __str__(self) -> str:
        return f"Data: {self.key}, {self.condition}, {self.status}"