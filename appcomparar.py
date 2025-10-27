# appcomparar.py
from flask import Flask, render_template, request
from sqlalchemy import create_engine
from TabelaComparer import TabelaComparer

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# -----------------------------
# Configuração dos bancos
# -----------------------------
# MySQL → senha em branco
mysql_user = "root"
mysql_password = ""
mysql_host = "localhost"
mysql_db = "data_lake_db"

# PostgreSQL → senha '123'
pg_user = "postgres"
pg_password = "123"
pg_host = "localhost"
pg_db = "data_lake_db"

# Cria as conexões
engine_mysql = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}")
engine_pg = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}")

# Instancia o comparador
comparer = TabelaComparer(engine_mysql, engine_pg)

# -----------------------------
# Rotas Flask
# -----------------------------
@app.route("/")
def index():
    return render_template("comparar.html", resultado=None)

@app.route("/comparar", methods=["GET", "POST"])
def comparar():
    tabela = request.args.get("tabela") or request.form.get("tabela")
    if not tabela:
        return render_template("comparar.html", resultado=None, erro="Informe o nome da tabela.")

    resultado = comparer.comparar_tabela(tabela)
    return render_template("comparar.html", resultado=resultado, tabela=tabela)

if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
