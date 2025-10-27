# TabelaComparer.py
import pandas as pd
import traceback
from sqlalchemy import text

class TabelaComparer:
    def __init__(self, engine_mysql, engine_postgres):
        """
        engine_mysql: conexão SQLAlchemy para o MySQL
        engine_postgres: conexão SQLAlchemy para o PostgreSQL
        """
        self.engine_mysql = engine_mysql
        self.engine_postgres = engine_postgres

    def comparar_tabela(self, tabela):
        """
        Compara os registros de uma tabela entre MySQL e PostgreSQL.
        Retorna:
          - se são iguais
          - registros só no MySQL
          - registros só no PostgreSQL
        """
        try:
            # Lê os dados
            df_mysql = pd.read_sql(text(f"SELECT * FROM {tabela}"), con=self.engine_mysql)
            df_pg = pd.read_sql(text(f"SELECT * FROM {tabela}"), con=self.engine_postgres)

            # Se colunas forem diferentes, já retorna
            if set(df_mysql.columns) != set(df_pg.columns):
                return {
                    "iguais": False,
                    "mensagem": f"❌ Estrutura diferente. Colunas distintas.",
                    "colunas_mysql": list(df_mysql.columns),
                    "colunas_postgres": list(df_pg.columns),
                    "somente_mysql": [],
                    "somente_postgres": []
                }

            # Ordena colunas para comparar
            df_mysql = df_mysql[sorted(df_mysql.columns)]
            df_pg = df_pg[sorted(df_pg.columns)]

            # Detectar registros exclusivos (comparando todas as colunas)
            merged = df_mysql.merge(df_pg, how="outer", indicator=True)

            somente_mysql = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
            somente_postgres = merged[merged["_merge"] == "right_only"].drop(columns=["_merge"])

            iguais = len(somente_mysql) == 0 and len(somente_postgres) == 0

            return {
                "iguais": iguais,
                "mensagem": "✅ Tabelas idênticas." if iguais else "⚠️ Existem diferenças de registros.",
                "colunas": list(df_mysql.columns),
                "somente_mysql": somente_mysql.to_dict(orient="records"),
                "somente_postgres": somente_postgres.to_dict(orient="records"),
            }

        except Exception as e:
            return {
                "iguais": False,
                "mensagem": f"Erro ao comparar: {str(e)}\n{traceback.format_exc()}",
                "colunas": [],
                "somente_mysql": [],
                "somente_postgres": [],
            }
