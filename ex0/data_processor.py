from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self.calculator = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if self._data:
            value = self._data.pop(0)
            rank = self.calculator
            self.calculator += 1
            return (rank, value)
        else:
            raise Exception("No data available")


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list) and data:
            if all(isinstance(i, (int, float))
                   and not isinstance(i, bool) for i in data):
                return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data):
            if isinstance(data, list) and data:
                for i in data:
                    self._data.append(str(i))
            else:
                self._data.append(str(data))
        else:
            raise Exception("Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list) and data:
            if all(isinstance(i, str) for i in data):
                return True
            else:
                return False
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for i in data:
                    self._data.append(i)
            else:
                self._data.append(data)
        else:
            raise Exception("Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def logchechcer(log_data: dict[str, str]) -> bool:
            if isinstance(log_data, dict):
                if all(isinstance(k, str) for k in log_data):
                    if all(isinstance(v, str) for v in log_data.values()):
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return False

        if isinstance(data, dict):
            return logchechcer(data)
        elif isinstance(data, list) and data:
            return all(logchechcer(k) for k in data)
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data):
            if isinstance(data, dict):
                self._data.append(": ".join(data.values()))
            else:
                for d in data:
                    self._data.append(": ".join(d.values()))
        else:
            raise Exception("Improper log data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print("\nTesting Numeric Processor...")
    num1 = NumericProcessor()
    print(f"Trying to validate input '42': {num1.validate(42)}")
    num2 = NumericProcessor()
    print(f"Trying to validate input 'Hello': {num2.validate('Hello')}")
    try:
        print("Test invalid ingestion of string "
              "'foo' without prior validation:")
        num1.ingest('foo')  # type: ignore
    except Exception as e:
        print(f"Got exception: {e}")
    num_list = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    try:
        num_list.ingest(data)  # type: ignore
        print(f"Processing data: {data}")
        print("Extracting 3 values...")
        for i in range(0, 3):
            o = num_list.output()
            print(f"Numeric value {o[0]}: {o[1]}")
    except Exception as e:
        print(f"Got exception: {e}")
    print("\nTesting Text Processor...")
    text1 = TextProcessor()
    print(f"Trying to validate input '42': {text1.validate(42)}")
    text_list = TextProcessor()
    data2 = ['Hello', 'Nexus', 'World']
    try:
        print(f"Processing data: {data2}")
        text_list.ingest(data2)
        print("Extracting 1 value...")
        o1 = text_list.output()
        print(f"Text value {o1[0]}: {o1[1]}")
    except Exception as e:
        print(f"Got exception: {e}")
    print("\nTesting Log Processor...")
    log1 = LogProcessor()
    print(f"Trying to validate input 'Hello': {log1.validate('Hello')}")
    data1 = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
             {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f"Processing data: {data1}")
    try:
        log1.ingest(data1)
        print("Extracting 2 values...")
        o2 = log1.output()
        print(f"Log entry {o2[0]}: {o2[1]}")
        o3 = log1.output()
        print(f"Log entry {o3[0]}: {o3[1]}")
    except Exception as e:
        print(f"Got exception: {e}")
