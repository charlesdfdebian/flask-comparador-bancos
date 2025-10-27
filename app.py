from flask import Flask, render_template, request, redirect, url_for, flash
from hdfs_manager import CriarDiretorioHadoop
from database import DatabaseManager
import os
import pandas as pd
import json
from io import BytesIO
import subprocess

#desenvolvido por Charles Junqueira
app = Flask(__name__)
app.secret_key = "secret-key"

TABELAS = ["clientes", "vendas", "feedback", "dados_completos"]

db = DatabaseManager()
hdfs = CriarDiretorioHadoop()
HDFS_DIR = "/data-lake"

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/incluir", methods=["POST"])
def incluir():
    try:
        # 1️⃣ Pega o diretório local do formulário
        local_dir = request.form.get("local_dir")
        if not os.path.exists(local_dir):
            flash(f"⚠️ Diretório local '{local_dir}' não encontrado.")
            return redirect(url_for("index"))

        # 2️⃣ Cria diretório no HDFS (se ainda não existir)
        hdfs.criar_diretorio(HDFS_DIR)

        # 3️⃣ Copia arquivos do local para o HDFS
        hdfs.copy_files(local_dir, HDFS_DIR)

        # 4️⃣ Ler arquivos do HDFS
        # Excel
        clientes_hdfs = subprocess.check_output([hdfs.hadoop_bin, "dfs", "-cat", f"{HDFS_DIR}/clientes.xlsx"])
        clientes = pd.read_excel(BytesIO(clientes_hdfs))

        # CSV
        vendas_hdfs = subprocess.check_output([hdfs.hadoop_bin, "dfs", "-cat", f"{HDFS_DIR}/vendas.csv"])
        vendas = pd.read_csv(BytesIO(vendas_hdfs))

        # JSON
        feedback_hdfs = subprocess.check_output([hdfs.hadoop_bin, "dfs", "-cat", f"{HDFS_DIR}/feedback.json"])
        feedback = pd.DataFrame(json.loads(feedback_hdfs.decode('utf-8')))

        # 5️⃣ Inserir no MySQL
        db.inserir_dataframe(clientes, "clientes", if_exists="replace")
        db.inserir_dataframe(vendas, "vendas", if_exists="replace")
        db.inserir_dataframe(feedback, "feedback", if_exists="replace")

        # 6️⃣ Merge e limpeza de dados
        clientes_vendas = pd.merge(clientes, vendas, left_on='id', right_on='cliente_id', how='left')
        dados_completos = pd.merge(clientes_vendas, feedback, left_on='id', right_on='cliente_id', how='left')
        dados_filtrados = dados_completos[(dados_completos['id_venda'].notna()) | (dados_completos['nota'].notna())].copy()
        dados_filtrados = dados_filtrados.dropna()
        db.inserir_dataframe(dados_filtrados, "dados_completos", if_exists="replace")

        flash("✅ Dados incluídos com sucesso!")
    except Exception as e:
        flash(f"❌ Erro ao incluir dados: {str(e)}")
    return redirect(url_for("index"))

@app.route("/excluir", methods=["POST"])
def excluir():
    try:
        db.limpar_registros(TABELAS)
        flash("✅ Todos os registros foram excluídos!")
    except Exception as e:
        flash(f"❌ Erro: {str(e)}")
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True)
