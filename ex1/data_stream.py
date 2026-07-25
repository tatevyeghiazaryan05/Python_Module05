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


class DataStream():
    pass


if __name__ == "__main__":
    pass