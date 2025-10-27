import os
import subprocess

class CriarDiretorioHadoop:
    def __init__(self, hadoop_bin="/usr/local/hadoop/bin/hdfs"):
        self.hadoop_bin = hadoop_bin

    def path_exists(self, path):
        result = subprocess.run(
            [self.hadoop_bin, "dfs", "-test", "-e", path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return result.returncode == 0

    def criar_diretorio(self, path):
        if not self.path_exists(path):
            print(f"📁 Criando diretório {path} no HDFS...")
            subprocess.run([self.hadoop_bin, "dfs", "-mkdir", "-p", path], check=True)
        else:
            print(f"✅ Diretório {path} já existe no HDFS.")

    def copy_files(self, local_dir, hdfs_dir):
        if not os.path.exists(local_dir):
            print(f"⚠️ Diretório local {local_dir} não encontrado.")
            return
        for file_name in os.listdir(local_dir):
            local_file = os.path.join(local_dir, file_name)
            if os.path.isfile(local_file):
                print(f"⬆️ Enviando {file_name} → {hdfs_dir}/")
                subprocess.run([self.hadoop_bin, "dfs", "-put", "-f", local_file, hdfs_dir], check=True)
