import glob
import duckdb
import os
import time
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

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

    con.close()
    print("\n Database ready!")

def convert_csv_to_parquet(domain, encode='utf-8'):

    print("Connecting to in-memory database")

    with duckdb.connect(":memory:") as con:

        for year in range(2009, 2019):
            csv_file = f'./{domain}-datasets/{domain}-{year}.csv'
            parquet_file = f'./{domain}-datasets/{domain}-{year}.parquet'

            if os.path.exists(parquet_file):
                print(f"\n--- {parquet_file} already exists. Skipping conversion for {year}... ---")
                continue

            if os.path.exists(csv_file):
                print(f"Loading {csv_file}...")
                start_time = time.time()

                query = f"SELECT * FROM read_csv_auto('{csv_file}', encoding='{encode}', delim=';', header=True, ignore_errors=True, store_rejects=True)"

                con.sql(query).write_parquet(parquet_file)

                print(f"Saved to {parquet_file}")
            
                # Calculate metrics
                elapsed_time = time.time() - start_time
                parquet_size_mb = os.path.getsize(parquet_file) / (1024 * 1024)
                
                print(f"SUCCESS: Saved {parquet_size_mb:.2f} MB to {parquet_file}")
                print(f"Time taken: {elapsed_time:.2f} seconds")

                # Safely check for rejected rows
                try:
                    rejects_count = con.sql("SELECT count(*) FROM reject_errors").fetchone()[0]
                    if rejects_count > 0:
                        print(f"WARNING: {rejects_count} rows were skipped due to parsing or encoding errors.")
                        print(con.sql("SELECT line, error_message FROM reject_errors LIMIT 3").df())
                except duckdb.CatalogException:
                    # If the table doesn't exist, it means 0 errors occurred!
                    pass

            else:
                print(f"\nFile {csv_file} not found. Skipping...")

def convert_pib_csv_to_parquet_with_ibge(domain='pib', encode='iso-8859_9-1999'):
    
    print("Connecting to in-memory database")

    with duckdb.connect(":memory:") as con:

        for year in range(2009, 2019):
            csv_file = f'./{domain}-datasets/{domain}-{year}.csv'
            parquet_file = f'./{domain}-datasets/{domain}-{year}.parquet'
            ibge_file = 'codigos_municipios_regioes.csv'

            if os.path.exists(parquet_file):
                print(f"\n--- {parquet_file} already exists. Skipping conversion for {year}... ---")
                continue

            if os.path.exists(csv_file):
                print(f"\nLoading {csv_file}...")
                start_time = time.time()

                query = rf"""
                    WITH ibge_lookup AS (
                        SELECT 
                            CAST(cod_ibge AS INT) AS cod_ibge,
                            UPPER(TRIM(regexp_replace(strip_accents(REPLACE(municipio, '-', ' ')), '\s+', ' ', 'g'))) AS municipio_norm
                        FROM read_csv(
                            '{ibge_file}', 
                            header=True, 
                            encoding='iso-8859_9-1999',     
                            ignore_errors=True
                        )
                        WHERE length(CAST(cod_ibge AS VARCHAR)) > 4
                    ),
                    pib_data AS (
                        SELECT 
                            {year} AS ano_exercicio,
                            column00 AS ds_municipio,
                            column01 AS agropecuaria,
                            column02 AS industria,
                            column03 AS servicos,
                            column04 AS adm_publica,
                            column05 AS total_excl_adm,
                            column06 AS impostos,
                            column07 AS pib_total,
                            column08 AS pib_per_capita
                        FROM read_csv(
                            '{csv_file}',
                            delim = ';',
                            decimal_separator = ',',
                            skip = 10,
                            header = false,
                            encoding = '{encode}',
                            ignore_errors = true
                        )
                        -- NOVO WHERE: Ignora nulos, vazios e as linhas de rodapé
                        WHERE column00 IS NOT NULL 
                          AND column00 != ''
                          AND column00 NOT LIKE 'Fonte:%'
                          AND column00 NOT LIKE '(1)%'
                          AND column00 NOT LIKE '(2)%'
                          AND column00 NOT LIKE 'Nota:%'
                    )
                    SELECT 
                        ibge.cod_ibge,
                        p.*
                    FROM pib_data p
                    LEFT JOIN ibge_lookup ibge 
                        ON UPPER(TRIM(regexp_replace(strip_accents(
                            REPLACE(
                                REPLACE(
                                    REPLACE(p.ds_municipio, '-', ' '), -- 1. Troca hífen por espaço
                                'Florínia', 'Florínea'),               -- 2. Corrige o I para E
                            'São Luís do', 'São Luiz do')              -- 3. Corrige o S para Z
                        ), '\s+', ' ', 'g'))) = ibge.municipio_norm
                """

                con.sql(query).write_parquet(parquet_file)
            
                elapsed_time = time.time() - start_time
                parquet_size_mb = os.path.getsize(parquet_file) / (1024 * 1024)
                
                print(f"SUCCESS: Saved {parquet_size_mb:.2f} MB to {parquet_file}")
                print(f"Time taken: {elapsed_time:.2f} seconds")

                try:
                    rejects_count = con.sql("SELECT count(*) FROM reject_errors").fetchone()[0]
                    if rejects_count > 0:
                        print(f"WARNING: {rejects_count} rows were skipped due to parsing or encoding errors.")
                except duckdb.CatalogException:
                    pass

            else:
                print(f"\nFile {csv_file} not found. Skipping...")

def add_ibge_to_despesas_parquets(domain='despesas'):
    print(f"Iniciando injeção de COD_IBGE nos Parquets de {domain.capitalize()}...\n")

    with duckdb.connect(":memory:") as con:
        for year in range(2009, 2019):
            parquet_original = f'./{domain}-datasets/{domain}-{year}.parquet'
            parquet_novo = f'./{domain}-datasets/{domain}-{year}-ibge.parquet'

            if not os.path.exists(parquet_original):
                print(f"Arquivo {parquet_original} não encontrado. Pulando...")
                continue
                
            if os.path.exists(parquet_novo):
                print(f"--- {parquet_novo} já existe. Pulando... ---")
                continue

            print(f"Processando ano {year}...")
            start_time = time.time()

            # Note o 'rf' no início para o Regex funcionar perfeitamente
            query = rf"""
                WITH ibge_lookup AS (
                    SELECT 
                        CAST(cod_ibge AS INT) AS cod_ibge,
                        -- Vacina do IBGE: Limpa acentos, hífens e apóstrofos
                        UPPER(TRIM(regexp_replace(strip_accents(REPLACE(REPLACE(municipio, '-', ' '), '''', ' ')), '\s+', ' ', 'g'))) AS municipio_norm
                    FROM read_csv('codigos_municipios_regioes.csv', header=True, encoding='iso-8859_9-1999', ignore_errors=True)
                    WHERE length(CAST(cod_ibge AS VARCHAR)) > 4
                )
                SELECT 
                    ibge.cod_ibge,
                    d.*
                FROM read_parquet('{parquet_original}') d
                LEFT JOIN ibge_lookup ibge 
                    -- Vacina das Despesas: Corrige Florínea, São Luiz e limpa pontuações
                    ON UPPER(TRIM(regexp_replace(strip_accents(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(d.ds_municipio, 'Florínia', 'Florínea'), 
                                'São Luís do', 'São Luiz do'),
                            '-', ' '),
                        '''', ' ')
                    ), '\s+', ' ', 'g'))) = ibge.municipio_norm
            """

            # Executa a query e exporta para o novo Parquet instantaneamente
            con.sql(query).write_parquet(parquet_novo)

            elapsed_time = time.time() - start_time
            parquet_size_mb = os.path.getsize(parquet_novo) / (1024 * 1024)
            print(f"SUCCESS: Salvo {parquet_size_mb:.2f} MB em {parquet_novo} ({elapsed_time:.2f}s)\n")

def gerar_parquets_capital():
    folder = './despesas-datasets/'
    with duckdb.connect(":memory:") as con:
        for year in range(2009, 2019):
            csv_path = f'{folder}sp-despesas-{year}.csv'
            parquet_path = f'{folder}despesas-{year}-capital.parquet'
            
            if not os.path.exists(csv_path): 
                print(f"\nArquivo {csv_path} não encontrado. Pulando...")
                continue
            
            tabela_erros = f"rejeicoes_{year}_erros"
            tabela_scans = f"rejeicoes_{year}_scans"
            
            query = f"""
                COPY (
                    SELECT 
                        3550308::INTEGER AS cod_ibge,
                        "Exercício"::BIGINT AS ano_exercicio,
                        'SÃO PAULO' AS ds_municipio,
                        EXTRACT(MONTH FROM "Data do empenho"::DATE) AS mes_referencia,
                        "Valor do empenho"::VARCHAR AS vl_despesa,
                        "Descrição da função" AS ds_funcao_governo,
                        "Descrição da subfunção" AS ds_subfuncao_governo,
                        "Código do programa do governo"::BIGINT AS cd_programa,
                        "Descrição do programa do governo" AS ds_programa,
                        "Código do projeto/atividade"::BIGINT AS cd_acao,
                        "Descrição do projeto/atividade" AS ds_acao,
                        "Descrição da fonte de recurso" AS ds_fonte_recurso,
                        "Descrição do elemento de despesa" AS ds_elemento
                    FROM read_csv(
                        '{csv_path}', 
                        header=True, 
                        sep=',', 
                        encoding='cp850', 
                        quote='"',
                        escape='"',
                        all_varchar=True,      
                        strict_mode=False,     
                        ignore_errors=True,    
                        null_padding=True,
                        store_rejects=True,
                        rejects_table='{tabela_erros}',
                        rejects_scan='{tabela_scans}'
                    )
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """
            
            try:
                con.execute(query)
                print(f"✅ Capital {year} processada com sucesso.")
                
                try:
                    rejects_count = con.sql(f"SELECT count(*) FROM {tabela_erros}").fetchone()[0]
                    if rejects_count > 0:
                        print(f"   ⚠️ WARNING: {rejects_count} linhas ignoradas.")
                except duckdb.CatalogException:
                    pass
                    
            except Exception as e:
                print(f"❌ Erro crítico na Capital {year}: {e}")

def merge_capital_interior_por_ano():
    folder = './despesas-datasets/'
    
    colunas_finais = [
        "cod_ibge",
        "ano_exercicio",
        "ds_municipio",
        "mes_referencia",
        "ds_funcao_governo",
        "ds_subfuncao_governo",
        "cd_programa",
        "ds_programa",
        "cd_acao",
        "ds_acao",
        "ds_fonte_recurso",
        "ds_elemento"
    ]
    
    colunas_sql = ", ".join(colunas_finais)
    
    with duckdb.connect(":memory:") as con:
        print("🚀 Iniciando o merge anual (Seleção explícita de features)...")
        
        for year in range(2009, 2019):
            arq_interior = f'{folder}despesas-{year}.parquet'
            arq_capital = f'{folder}despesas-{year}-capital.parquet'
            arq_saida = f'{folder}despesas-{year}-estado.parquet'
            
            if not os.path.exists(arq_interior) or not os.path.exists(arq_capital):
                continue
            
            query = f"""
                COPY (
                    SELECT 
                        {colunas_sql},
                        -- Sanitização do valor
                        TRY_CAST(REPLACE(vl_despesa, ',', '.') AS DOUBLE) AS vl_despesa
                    FROM read_parquet([
                        '{arq_interior}', 
                        '{arq_capital}'
                    ], union_by_name=true)
                ) TO '{arq_saida}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
            """
            
            try:
                con.execute(query)
                linhas = con.sql(f"SELECT count(*) FROM read_parquet('{arq_saida}')").fetchone()[0]
                print(f"✅ {year} unificado | Total: {linhas:,}".replace(',', '.'))
                
            except Exception as e:
                print(f"❌ Erro ao mesclar o ano {year}: {e}")

def filtrar_parquets_estado():
    folder = './despesas-datasets/'
    
    colunas_finais = [
        "cod_ibge",
        "ano_exercicio",
        "ds_municipio",
        "mes_referencia",
        "ds_funcao_governo",
        "ds_subfuncao_governo",
        "cd_programa",
        "ds_programa",
        "cd_acao",
        "ds_acao",
        "ds_fonte_recurso",
        "ds_elemento"
    ]
    
    colunas_sql = ", ".join(colunas_finais)
    
    with duckdb.connect(":memory:") as con:
        print("🚀 Iniciando a filtragem dos arquivos recuperados (Redução de features)...")
        
        for year in range(2009, 2019):
            arq_entrada = f'{folder}despesas-{year}-estado.parquet'
            arq_temporario = f'{folder}despesas-{year}-estado-limpo.parquet'
            
            if not os.path.exists(arq_entrada):
                print(f"⚠️ {arq_entrada} não encontrado. Pulando...")
                continue
            
            # Lê do estado, filtra colunas, higieniza valor e salva no temporário
            query = f"""
                COPY (
                    SELECT 
                        {colunas_sql},
                        -- CAST(vl_despesa AS VARCHAR) previne erro caso a coluna já tenha sido convertida antes
                        TRY_CAST(REPLACE(CAST(vl_despesa AS VARCHAR), ',', '.') AS DOUBLE) AS vl_despesa
                    FROM read_parquet('{arq_entrada}')
                ) TO '{arq_temporario}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
            """
            
            try:
                con.execute(query)
                linhas = con.sql(f"SELECT count(*) FROM read_parquet('{arq_temporario}')").fetchone()[0]
                
                # O Truque do Engenheiro: Substituir o arquivo velho pelo novo automaticamente
                os.remove(arq_entrada) # Deleta o arquivo de 270MB
                os.rename(arq_temporario, arq_entrada) # O arquivo limpo assume o nome original
                
                print(f"✅ {year} filtrado e substituído | Total: {linhas:,}".replace(',', '.'))
                
            except Exception as e:
                print(f"❌ Erro ao processar o ano {year}: {e}")

def consolidar_decada_por_dominio():

    dominios = ['despesas', 'pib']
    
    with duckdb.connect(":memory:") as con:
        
        con.execute("PRAGMA memory_limit='12GB'")
        
        con.execute("PRAGMA temp_directory='./duckdb_tmp'")
        
        for dominio in dominios:
            pasta = f'./{dominio}-datasets/'
            arquivo_final = f'./{dominio}-2009-2018.parquet'
            
            print(f"🚀 Iniciando a consolidação da década para o domínio: {dominio.upper()}...")
            
            query = f"""
                COPY (
                    SELECT * 
                    FROM read_parquet('{pasta}{dominio}-20*.parquet', union_by_name=true)
                ) TO '{arquivo_final}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
            """
            
            try:
                con.execute(query)
                
                linhas = con.sql(f"SELECT count(*) FROM read_parquet('{arquivo_final}')").fetchone()[0]
                print(f"✅ {dominio.upper()} consolidado com sucesso!")
                print(f"📊 Arquivo gerado: {arquivo_final}")
                print(f"📈 Total de linhas na década: {linhas:,}\n".replace(',', '.'))
                
            except Exception as e:
                print(f"❌ Erro ao consolidar {dominio}: {e}\n")


def run_eda_despesas():
    file_path = './despesas-datasets/despesas-2009-2018.parquet'
    output_dir = './eda_results'
    os.makedirs(output_dir, exist_ok=True)
    
    con = duckdb.connect()

    # O dataset tem 229 milhões de linhas,
    # para não estourar o consumo de RAM é necessário definir um limite

    con.execute("PRAGMA memory_limit='8GB'")
    
    total_rows = con.execute(f"SELECT count(*) FROM '{file_path}'").fetchone()[0]
    print(f"Total Rows: {total_rows:,}")

    print("\n missing values: ")
    null_checks = con.query(f"""
        SELECT 
            COUNT(*) - COUNT(ds_funcao_governo) AS missing_funcao,
            COUNT(*) - COUNT(mes_referencia) AS missing_mes,
            COUNT(*) - COUNT(vl_despesa) AS missing_valor
        FROM '{file_path}'
    """).df()
    print(null_checks.T)

    print("\n expense values: ")
    stats = con.query(f"""
        SELECT 
            MIN(vl_despesa) as min_val,
            MAX(vl_despesa) as max_val,
            AVG(vl_despesa) as mean_val,
            -- Counting negative values (Cancellations/Estornos we discussed earlier)
            SUM(CASE WHEN vl_despesa < 0 THEN 1 ELSE 0 END) as negative_rows 
        FROM '{file_path}'
    """).df()
    print(stats)


if __name__ == "__main__":

    #convert_csv_to_parquet(domain='despesas', encode='iso-8859_9-1999')
    #convert_pib_csv_to_parquet_with_ibge(domain='pib', encode="utf-8")

    #add_ibge_to_despesas_parquets(domain='despesas')
    #gerar_parquets_capital()
    #filtrar_parquets_estado()
    #consolidar_decada_por_dominio()
    run_eda_despesas()

