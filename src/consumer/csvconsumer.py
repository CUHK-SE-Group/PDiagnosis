from pathlib import Path

import pandas as pd


class CSVConsumer:
    def __init__(self, file_path: Path):
        self.data = pd.concat([pd.read_csv(file_path), pd.read_csv(str(file_path).replace("normal", "abnormal"))])
        if "Timestamp" in self.data.columns:
            self.data["Timestamp"] = pd.to_datetime(self.data["Timestamp"]).astype('int64') // 1e9
            self.data.sort_values(by="Timestamp", inplace=True)
        else:
            self.data["TimeUnix"] = pd.to_datetime(self.data["TimeUnix"]).astype('int64') // 1e9
            self.data.sort_values(by="TimeUnix", inplace=True)
