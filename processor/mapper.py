import pandas as pd

# -------------------------------------------------
# Schema de mapeamento (modelo interno do sistema)
# -------------------------------------------------
MAPPER_SCHEMA = {
    "Nome": ["Name", "name", "Nome"],
    "Ticker": ["Símbolo", "symbol", "Ticker"],
    "Mercado": ["Mercado"],
    "Último_Preço": ["Último (Preço)"],
    "Variacao_%": ["%"],
    "Data_Hora": ["Data/Hora"],
    "Link": ["Link"],
}

# -------------------------------------------------
# Funções de mapeamento
# -------------------------------------------------

def first_existing_column(df, columns):
    """
    Retorna a primeira coluna existente no DataFrame.
    Se nenhuma existir, retorna uma Series vazia (None).
    """
    for col in columns:
        if col in df.columns:
            return df[col]
    return pd.Series([None] * len(df))


def map_dataframe(df, schema=MAPPER_SCHEMA):
    """
    Mapeia um DataFrame de entrada para o modelo interno
    definido no schema.
    """
    mapped = pd.DataFrame()

    for target_col, source_cols in schema.items():
        mapped[target_col] = first_existing_column(df, source_cols)

    return mapped
