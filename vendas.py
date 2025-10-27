from hdfs_base import HDFSReader

class Vendas(HDFSReader):
    def __init__(self, path="/data-lake/vendas.csv", hdfs_bin="/usr/local/hadoop/bin/hdfs"):
        super().__init__(path, hdfs_bin)
        self.tipo = "csv"
