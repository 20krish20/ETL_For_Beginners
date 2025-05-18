from datetime import datetime

class Logger:
    def __init__(self, log_file="log_file.txt"):
        self.log_file = log_file


    def log(self, message):
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        time_now = datetime.now()
        timestamp = time_now.strftime(timestamp_format)
        with open(self.log_file, "a") as f:
            f.write(timestamp + ',' + message + '\n')