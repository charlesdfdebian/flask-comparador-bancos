from sqlalchemy import create_engine, text

class DatabaseManager:
    def __init__(self, user=None, password=None, host=None, database=None):
        self.user = user or "root"
        self.password = password or ""
        self.host = host or "localhost"
        self.database = database or "data_lake_db"
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
        )

    def inserir_dataframe(self, df, tabela, if_exists='append'):
        df.to_sql(tabela, con=self.engine, if_exists=if_exists, index=False)
        print(f"✅ Dados inseridos na tabela '{tabela}'.")

    def limpar_registros(self, tabelas):
        with self.engine.begin() as conn:  # garante commit automático
            for tabela in tabelas:
                conn.execute(text(f"DELETE FROM {tabela};"))
                print(f"🗑️ Todos os registros da tabela '{tabela}' foram excluídos.")
        print("✅ Todos os registros foram removidos com sucesso.")
