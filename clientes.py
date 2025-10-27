from hdfs_base import HDFSReader

class Clientes(HDFSReader):
    def __init__(self, path="/data-lake/clientes.xlsx", hdfs_bin="/usr/local/hadoop/bin/hdfs"):
        super().__init__(path, hdfs_bin)
        self.tipo = "excel"
