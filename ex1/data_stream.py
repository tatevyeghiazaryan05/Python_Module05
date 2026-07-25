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
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if isinstance(proc, DataProcessor):
            self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    processed = True
                    break
            if not processed:
                print(f"DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
        else:
            for proc in self.processors:
                if isinstance(proc, NumericProcessor):
                    proc_name = "Numeric Processor"
                elif isinstance(proc, TextProcessor):
                    proc_name = "Text Processor"
                elif isinstance(proc, LogProcessor):
                    proc_name = "Log Processor"
                remaining = len(proc._data)
                total = proc.calculator + len(proc._data)
                print(f"{proc_name}: total {total} items processed, "
                      f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    data = DataStream()
    data.print_processors_stats()
    batch = ['Hello world', [3.14, -1, 2.71],
             [{'log_level': 'WARNING',
               'log_message': 'Telnet access! Use ssh instead'},
              {'log_level': 'INFO',
               'log_message': 'User wil is connected'}],
             42, ['Hi', 'five']]
    print("\nRegistering Numeric Processor")
    print(f"\nSend first batch of data on stream: {batch}")
    num_proc = NumericProcessor()
    data.register_processor(num_proc)
    data.process_stream(batch)
    data.print_processors_stats()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    print("\nRegistering other data processors")
    print("Send the same batch again")
    data.register_processor(text_proc)
    data.register_processor(log_proc)
    data.process_stream(batch)
    data.print_processors_stats()
    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    num_proc.output()
    num_proc.output()
    num_proc.output()
    text_proc.output()
    text_proc.output()
    log_proc.output()
    data.print_processors_stats()
