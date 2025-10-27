from hdfs_base import HDFSReader

class Feedback(HDFSReader):
    def __init__(self, path="/data-lake/feedback.json", hdfs_bin="/usr/local/hadoop/bin/hdfs"):
        super().__init__(path, hdfs_bin)
        self.tipo = "json"
