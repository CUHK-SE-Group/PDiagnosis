import pandas as pd
class CSVConsumer():
    def __init__(self, file_path):
        self.data = pd.read_csv(file_path)
        if "Timestamp" in self.data.columns:
            self.data["Timestamp"] = pd.to_datetime(self.data["Timestamp"]).astype('int64') // 1e9
        else:
            self.data["TimeUnix"] = pd.to_datetime(self.data["TimeUnix"]).astype('int64') // 1e9
