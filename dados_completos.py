import pandas as pd

class DadosCompletos:
    def __init__(self, clientes_df, vendas_df, feedback_df):
        self.clientes_df = clientes_df
        self.vendas_df = vendas_df
        self.feedback_df = feedback_df
        self.df_completo = None

    def unir_dados(self):
        clientes_vendas = pd.merge(self.clientes_df, self.vendas_df, left_on='id', right_on='cliente_id', how='left')
        dados_completos = pd.merge(clientes_vendas, self.feedback_df, left_on='id', right_on='cliente_id', how='left')
        dados_filtrados = dados_completos[
            (dados_completos['id_venda'].notna()) | (dados_completos['nota'].notna())
        ].copy()
        self.df_completo = dados_filtrados.dropna()
        return self.df_completo
