import subprocess
import pandas as pd
from io import StringIO
import json

class HDFSReader:
    def __init__(self, path, hdfs_bin="/usr/local/hadoop/bin/hdfs"):
        self.path = path
        self.hdfs_bin = hdfs_bin
        self.df = None
        self.tipo = None  # csv, excel ou json

    def ler_hdfs(self):
        if self.tipo == "excel":
            temp_path = "/tmp/temp_file.xlsx"
            subprocess.run([self.hdfs_bin, "dfs", "-get", "-f", self.path, temp_path], check=True)
            self.df = pd.read_excel(temp_path)
        else:
            output = subprocess.check_output([self.hdfs_bin, "dfs", "-cat", self.path])
            if self.tipo == "csv":
                self.df = pd.read_csv(StringIO(output.decode('utf-8')))
            elif self.tipo == "json":
                data = json.loads(output.decode('utf-8'))
                self.df = pd.DataFrame(data)
            else:
                raise ValueError(f"Tipo de arquivo '{self.tipo}' não suportado.")
        return self.df
