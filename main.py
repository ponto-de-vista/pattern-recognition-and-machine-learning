import glob
import duckdb
import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def process_criminal_data(file_path, output_enc='utf-8-sig'):
    try:
        logging.info(f"Analyzing workbook: {file_path}")
        
        excel = pd.ExcelFile(file_path, engine='openpyxl')
        
        # Precise filtering: Include only semester-based data sheets
        # This ignores metadata like 'Campos da Tabela_SPDADOS' automatically
        data_sheets = [s for s in excel.sheet_names if any(m in s.upper() for m in ["JAN", "JUL", "DEZ"])]
        
        if not data_sheets:
            logging.warning(f"No valid data sheets found in {file_path}. Skipping.")
            return

        all_data = []
        for sheet in data_sheets:
            logging.info(f"  -> Extracting: {sheet}")

            df = pd.read_excel(excel, sheet_name=sheet)
            
            df = df.dropna(how='all')
            all_data.append(df)

        # Merge and Export
        final_df = pd.concat(all_data, ignore_index=True)
        output_csv = file_path.rsplit('.', 1)[0] + "_clean.csv"
        
        # Using 'quoting' ensures that text with commas doesn't break your CSV structure
        final_df.to_csv(output_csv, index=False, encoding=output_enc, quoting=1)
        
        size_mb = os.path.getsize(output_csv) / (1024**2)
        logging.info(f"✅ Created: {output_csv} ({size_mb:.2f} MB)\n")

    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")

if __name__ == "__main__":

    """
    target_files = glob.glob('./criminal-datasets/*.xlsx')
    for f in target_files:
        process_criminal_data(f)
    """

    # pib dataset uses ;
    for f in glob.glob('./pib-datasets/*.csv'):
        query = f"SELECT * FROM read_csv('{f}', delim=';', encoding='latin-1') LIMIT 0"
        print(f, "\n", duckdb.query(query).df().columns.tolist(), "\n")

    """
        ano_exercicio;
        mes_referencia;
        ds_municipio;
        vl_despesa;
    """

    # criminal dataset uses ,
    for f in glob.glob('./criminal-datasets/*.csv'):
        # We use quote='"' because your columns are wrapped in double quotes
        # We ensure delim=',' because Python's to_csv defaults to comma
        query = f"SELECT * FROM read_csv('{f}', delim=',', quote='\"', encoding='utf-8') LIMIT 0"
        print(f, "\n", duckdb.query(query).df().columns.tolist(), "\n")

    """
        ANO_ESTATISTICA;
        MES_ESTATISTICA;
        CIDADE or NOME_MUNICIPIO;
        NATUREZA_APURADA;
    """
