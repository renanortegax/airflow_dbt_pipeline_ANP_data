#%%
import pandas as pd
import requests
# from io import StringIO
from io import BytesIO
from pathlib import Path
import logging
import datetime
from include.utils import dataset_names

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_SET = dataset_names()

URL_BASE = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/'

def extract_anp_data(output_dir):
    output_path = Path(output_dir)
    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)

    for dataset, filename in DATA_SET.items():
        url = URL_BASE + filename

        response = requests.get(url, timeout=30)
        logging.info("Status request %s", response.status_code)

        df = pd.read_csv(
            BytesIO(response.content),
            sep=';'
        )

        # path_export = output_path / f"{datetime.datetime.now().strftime('%Y%m%d')}_{dataset}.parquet"
        path_export = output_path / f"{dataset}.parquet"

        df['dataset'] = dataset
        df.to_parquet(
            path_export,
            index=False
        )

        logging.info('Extracao concluida %s. Salvo em %s', dataset, path_export)
    
if __name__ == '__main__':
    extract_anp_data('./data/raw/anp/')
