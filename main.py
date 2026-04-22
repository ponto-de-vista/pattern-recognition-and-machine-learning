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

def create_unified_database(db_path='pattern_recognition.duckdb'):
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '16GB'")
    con.execute("SET threads = 16")

    print("--- Importing Total Expenses (PIB) ---")
    for i, f in enumerate(glob.glob('./pib-datasets/*.csv')):
        if i == 0:
            con.execute("DROP TABLE IF EXISTS expenses")
            insert_mode = "CREATE TABLE expenses AS"
        else:
            insert_mode = "INSERT INTO expenses"
        
        con.execute(f"""
            {insert_mode}
            SELECT year, month, city, SUM(value) as total_value
            FROM (
                SELECT 
                    ano_exercicio::INT AS year,
                    mes_referencia::INT AS month,
                    UPPER(ds_municipio) AS city,
                    REPLACE(vl_despesa, ',', '.')::DOUBLE AS value
                FROM read_csv('{f}', delim=';', encoding='iso-8859_1-1998', header=true, ignore_errors=true)
            )
            GROUP BY year, month, city
        """)
        print(f"  Processed PIB: {f}")

    print("\n--- Importing Total Crime Counts (Aggregated) ---")
    con.execute("DROP TABLE IF EXISTS crimes")
    
    # Notice we pass the wildcard '*' directly into read_csv
    # and added union_by_name=true at the end!
    con.execute("""
        CREATE TABLE crimes AS
        SELECT year, month, city, COUNT(*) as total_crimes
        FROM (
            SELECT 
                ANO_ESTATISTICA::INT AS year,
                MES_ESTATISTICA::INT AS month,
                UPPER(COALESCE(CIDADE, NOME_MUNICIPIO)) AS city
            FROM read_csv('./criminal-datasets/*_clean.csv', 
                          delim=',', 
                          quote='\"', 
                          encoding='utf-8', 
                          header=true, 
                          ignore_errors=true,
                          union_by_name=true)
        )
        GROUP BY year, month, city
    """)
    print("  Processed ALL Crime files successfully in one go!")

    con.close()
    print("\n Database ready!")

if __name__ == "__main__":
    create_unified_database()
