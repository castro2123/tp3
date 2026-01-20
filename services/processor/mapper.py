import pandas as pd

MAPPER_SCHEMA = {
    "Nome": ["Name", "name", "Nome"],
    "Ticker": ["Símbolo", "symbol", "Ticker"],
    "Mercado": ["Mercado"],
    "Último_Preço": ["Último (Preço)"],
    "Variacao_%": ["%"],
    "Data_Hora": ["Data/Hora"],
    "Link": ["Link"],
}

def first_existing_column(df, columns):
    for col in columns:
        if col in df.columns:
            return df[col]
    return pd.Series([None] * len(df))

def map_dataframe(df, schema=MAPPER_SCHEMA):
    mapped = pd.DataFrame()
    for target_col, source_cols in schema.items():
        mapped[target_col] = first_existing_column(df, source_cols)
    return mapped
