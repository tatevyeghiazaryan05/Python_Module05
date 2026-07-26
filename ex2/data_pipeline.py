from typing import Any, Protocol
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


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
        print("\n== DataStream statistics ==")
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            datas = []
            for _ in range(nb):
                try:
                    added_data = proc.output()
                    datas.append(added_data)
                except Exception:
                    break
            if datas:
                plugin.process_output(datas)


class CSVExportPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if data:
            values = [d[1] for d in data]
            print("CSV Output:")
            print(",".join(values))


class JSONExportPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if data:
            json_items = [f'"item_{d[0]}": "{d[1]}"' for d in data]
            json_str = "{" + ', '.join(json_items) + "}"
            print("JSON Output:")
            print(json_str)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...")
    data = DataStream()
    data.print_processors_stats()
    print("\nRegistering Processors")
    batch1 = ['Hello world', [3.14, -1, 2.71],
              [
                {'log_level': 'WARNING',
                 'log_message': 'Telnet access! Use ssh instead'},
                {'log_level': 'INFO', 'log_message': 'User wil is connected'}
             ],
             42,
             ['Hi', 'five']
            ]
    print(f"\nSend first batch of data on stream: {batch1}")
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    data.register_processor(num_proc)
    data.register_processor(text_proc)
    data.register_processor(log_proc)
    data.process_stream(batch1)
    data.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    data.output_pipeline(3, csv_plugin)
    data.print_processors_stats()
    batch2 = [
              21,
              [
               'I love AI', 'LLMs are wonderful', 'Stay healthy'
              ],
              [
               {'log_level': 'ERROR', 'log_message': '500 server crash'},
               {
                   'log_level': 'NOTICE',
                   'log_message': 'Certificate expires in 10 days'
               }
              ],
              [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"\nSend another batch of data: {batch2}")
    data.process_stream(batch2)
    data.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    data.output_pipeline(5, json_plugin)
    data.print_processors_stats()
