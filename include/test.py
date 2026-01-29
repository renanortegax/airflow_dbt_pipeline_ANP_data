#%%
import pandas as pd
import os

os.listdir('../data/raw/anp')

#%%
df_20260127_diesel_gnv = pd.read_parquet('../data/raw/anp/20260127_diesel_gnv.parquet')
df_20260127_gasolina_etanol = pd.read_parquet('../data/raw/anp/20260127_gasolina_etanol.parquet')
df_20260127_glp = pd.read_parquet('../data/raw/anp/20260127_glp.parquet')

#%%
columns = df_20260127_diesel_gnv.columns.tolist()
[c.replace(" - ", " ").replace(" ","_").lower() for c in columns]
#%%
df_20260127_diesel_gnv['Data da Coleta'] = pd.to_datetime(df_20260127_diesel_gnv['Data da Coleta'], format="%d/%m/%Y").dt.normalize()
df_20260127_diesel_gnv.info()

#%%
df_20260127_diesel_gnv.head()

#%%
from utils import dataset_names

ds = dataset_names()

for D in ds.keys():
    print(D)


#%%
def treat_columns_name_raw_dataset(df):
    df.columns = [c.replace(" - ", " ").replace(" ","_").lower() for c in df.columns.tolist()]
    return df

treat_columns_name_raw_dataset(df)


#%%
from utils import dataset_names
import requests
from io import BytesIO
import pandas as pd

DATA_SET = dataset_names()
URL_BASE = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/'

url = URL_BASE + DATA_SET.get('diesel_gnv')
response = requests.get(url, timeout=30)

df = pd.read_csv(
    BytesIO(response.content),
    sep=';'
)

#%%
def treat_columns_name_raw_dataset(df):
    df.columns = [c.replace(" - ", " ").replace(" ","_").lower() for c in df.columns.tolist()]
    return df

print(df.columns.tolist())
df = treat_columns_name_raw_dataset(df)
print(df.columns.tolist())

df.info()
#%%
# df['valor_de_venda'] = df['valor_de_venda'].str.replace(",",".").astype('float')
# df['valor_de_compra'] = df['valor_de_compra'].str.replace(",",".").astype('float')
# df['data_da_coleta'] = pd.to_datetime(df_20260127_diesel_gnv['data_da_coleta'], format="%d/%m/%Y").dt.normalize()

for col in ["valor_de_venda", "valor_de_compra"]:
    if col in df.columns and df[col].dtype == "object":
        df[col] = (df[col].str.replace(",", ".", regex=False).astype("float"))

col_date = "data_da_coleta"

if col_date in df.columns and df[col_date].dtype == "object":
    df[col_date] = (pd.to_datetime(df[col_date], format="%d/%m/%Y", errors="raise").dt.normalize())

#%%
df.info()
#%%
