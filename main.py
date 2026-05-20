import glob
import duckdb
import os
import time
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import unicodedata

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

def convert_pib_csv_to_parquet_with_ibge(domain='pib', encode='utf-8'):
    
    print("Connecting to in-memory database")

    with duckdb.connect(":memory:") as con:

        for year in [2019]:
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
                            column0 AS ds_municipio,
                            TRY_CAST(column1 AS DOUBLE) AS agropecuaria,
                            TRY_CAST(column2 AS DOUBLE) AS industria,
                            TRY_CAST(column3 AS DOUBLE) AS servicos,
                            TRY_CAST(column4 AS DOUBLE) AS adm_publica,
                            TRY_CAST(column5 AS DOUBLE) AS total_excl_adm,
                            TRY_CAST(column6 AS DOUBLE) AS impostos,
                            TRY_CAST(column7 AS DOUBLE) AS pib_total,
                            TRY_CAST(column8 AS DOUBLE) AS pib_per_capita
                        FROM read_csv(
                            '{csv_file}',
                            delim = ',',             
                            decimal_separator = '.', 
                            skip = 10,
                            header = false,
                            encoding = '{encode}',
                            ignore_errors = true
                        )
                        WHERE column0 IS NOT NULL 
                        AND column0 != ''
                        AND column0 NOT LIKE 'Fonte:%'
                        AND column0 NOT LIKE '(1)%'
                        AND column0 NOT LIKE '(2)%'
                        AND column0 NOT LIKE 'Nota:%'
                        AND column0 != 'ESTADO DE SÃO PAULO'
                    )
                    SELECT 
                        ibge.cod_ibge,
                        p.*
                    FROM pib_data p
                    LEFT JOIN ibge_lookup ibge 
                        ON UPPER(TRIM(regexp_replace(strip_accents(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(p.ds_municipio, '-', ' '), 
                                    'Florínia', 'Florínea'),               
                                'São Luís do', 'São Luiz do'),             
                            '''', '')                                      
                        ), '\s+', ' ', 'g'))) = REPLACE(ibge.municipio_norm, '''', '')
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

    dominios = ['pib', 'crime']
    
    with duckdb.connect(":memory:") as con:
        
        con.execute("PRAGMA memory_limit='12GB'")
        
        con.execute("PRAGMA temp_directory='./duckdb_tmp'")
        
        for dominio in dominios:
            pasta = f'./{dominio}-datasets/'
            arquivo_final = f'./{dominio}-2010-2019.parquet'
            
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

def unified_crime_parquet():
    duckdb.execute("""
        COPY (
            SELECT * FROM read_parquet('./crime-datasets/crime-20*.parquet', union_by_name=true)
        ) TO './crime-datasets/crime-2009-2018.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """)

def describe_dataset_columns():
    con = duckdb.connect()

    con.execute("PRAGMA memory_limit='8GB'")

    files = [
        './gdvDespesasExcel-datasets/gdvDespesasExcel-2010_com_ibge.parquet', 
    ]

    for f in files:
        print(f"\n file: {f}")
        linhas = con.execute(f"SELECT count(*) FROM '{f}'").fetchone()[0]
        print(f"Total de linhas: {linhas:,}")

        schema = con.execute(f"DESCRIBE SELECT * FROM '{f}'").df()
        print(schema[['column_name', 'column_type']].to_string(index=False))

def cardinalidade_despesas():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    print("Calculando a cardinalidade REAL (Limpa) das colunas categóricas...\n")

    # Utilizando aspas duplas para mapear exatamente os nomes das colunas do seu Parquet
    query = """
        SELECT 
            COUNT(DISTINCT UPPER(TRIM("Órgão"))) AS qtd_orgaos_limpos,
            COUNT(DISTINCT UPPER(TRIM("Função"))) AS qtd_funcoes_limpas,
            COUNT(DISTINCT UPPER(TRIM("Sub Função"))) AS qtd_subfuncoes_limpas,
            COUNT(DISTINCT UPPER(TRIM("Programa"))) AS qtd_programas_limpos,
            COUNT(DISTINCT UPPER(TRIM("Ação"))) AS qtd_acoes_limpas,
            COUNT(DISTINCT UPPER(TRIM("Funcional Programática"))) AS qtd_func_programatica_limpas,
            COUNT(DISTINCT UPPER(TRIM("Município"))) AS qtd_municipios_limpos,
            COUNT(DISTINCT UPPER(TRIM("Despesa"))) AS qtd_despesas_limpas,
            COUNT(DISTINCT UPPER(TRIM("drs"))) AS qtd_drs_limpos,
            COUNT(DISTINCT UPPER(TRIM("r_saude"))) AS qtd_r_saude_limpos
        FROM './gdvDespesasExcel-datasets/gdvDespesasExcel-2010_com_ibge.parquet'
    """

    # Executa a query e converte o resultado para um DataFrame do Pandas
    resultado = con.execute(query).df()
    
    # Imprime transposto (.T) para facilitar a visualização em formato de lista
    print(resultado.T)


def extrair_dicionario_dados():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    print("Extraindo dicionário de dados limpo e filtrado...\n")

    def pegar_lista(coluna, filtro_extra=""):
        query = f"""
            SELECT DISTINCT UPPER(TRIM({coluna})) AS categoria_limpa
            FROM './gdvDespesasExcel-datasets/gdvDespesasExcel-2010_com_ibge.parquet' 
            WHERE {coluna} IS NOT NULL {filtro_extra}
            ORDER BY 1
        """
        return con.execute(query).df()['categoria_limpa'].tolist()

    # 1. Funções e Órgãos (Globais, sem filtro)
    funcoes = pegar_lista('"Função"')
    orgaos = pegar_lista('"Órgão"')

    # --- INÍCIO DOS DADOS FILTRADOS POR SEGURANÇA PÚBLICA ---
    trava_seguranca = 'AND ("Função" ILIKE \'%segurança%\' OR "Função" ILIKE \'%seguranca%\')'
    
    subfuncoes_seguranca = pegar_lista('"Sub Função"', trava_seguranca)
    programas_seguranca = pegar_lista('"Programa"', trava_seguranca)
    acoes_seguranca = pegar_lista('"Ação"', trava_seguranca)
    func_programatica_seguranca = pegar_lista('"Funcional Programática"', trava_seguranca)
    municipios_seguranca = pegar_lista('"Município"', trava_seguranca)
    despesas_seguranca = pegar_lista('"Despesa"', trava_seguranca)
    drs_seguranca = pegar_lista('"drs"', trava_seguranca)
    r_saude_seguranca = pegar_lista('"r_saude"', trava_seguranca)


    # --- ÁREA DE IMPRESSÃO ---
    print(f"🔹 FUNÇÕES REAIS ({len(funcoes)}):")
    print(funcoes, "\n")

    print(f"🔹 ÓRGÃOS REAIS ({len(orgaos)}):")
    print(orgaos, "\n")

    print(f"🔹 SUBFUNÇÕES (Segurança Pública) ({len(subfuncoes_seguranca)}):")
    for sf in subfuncoes_seguranca:
        print(f"  - {sf}")
    print("\n")

    print(f"🔹 PROGRAMAS (Segurança Pública) ({len(programas_seguranca)}):")
    print(programas_seguranca, "\n")

    print(f"🔹 AÇÕES (Segurança Pública) ({len(acoes_seguranca)}):")
    print(acoes_seguranca, "\n")

    print(f"🔹 FUNCIONAL PROGRAMÁTICA (Segurança Pública) ({len(func_programatica_seguranca)}):")
    print(func_programatica_seguranca, "\n")
    
    print(f"🔹 DESPESAS (Segurança Pública) ({len(despesas_seguranca)}):")
    print(despesas_seguranca, "\n")

    print(f"🔹 MUNICÍPIOS (Segurança Pública) ({len(municipios_seguranca)}):")
    print(municipios_seguranca, "\n")

    print(f"🔹 DRS - DEPARTAMENTOS REGIONAIS DE SAÚDE (Segurança Pública) ({len(drs_seguranca)}):")
    print(drs_seguranca, "\n")

    print(f"🔹 REDES DE SAÚDE (Segurança Pública) ({len(r_saude_seguranca)}):")
    print(r_saude_seguranca, "\n")


def auditar_seguranca_estrategica():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    print("🔍 Filtrando apenas as Subfunções Estratégicas de Segurança...\n")

    # Criamos a regra de filtro uma vez para não repetir nas 3 queries
    filtro_estrategico = """
        (ds_funcao_governo ILIKE '%segurança%' OR ds_funcao_governo ILIKE '%seguranca%')
        AND (
            ds_subfuncao_governo ILIKE '%policiamento%'
            OR ds_subfuncao_governo ILIKE '%defesa civil%'
            OR ds_subfuncao_governo ILIKE '%informação e inteligência%'
            OR ds_subfuncao_governo ILIKE '%tecnologia da informa%'
            OR ds_subfuncao_governo ILIKE '%tecnologia da informatização%'
        )
    """

    # 1. Nova contagem reduzida
    query_count = f"""
        SELECT 
            COUNT(DISTINCT ds_acao) AS qtd_acoes_estrategicas,
            COUNT(DISTINCT ds_elemento) AS qtd_elementos_estrategicos
        FROM './despesas-datasets/despesas-2009-2018.parquet'
        WHERE {filtro_estrategico}
    """
    
    resultado = con.execute(query_count).df()
    print("🎯 Nova Cardinalidade (Pós-Filtro de Subfunção):")
    print(resultado.T)
    print("-" * 50)

    # 2. Extraímos a lista de ELEMENTOS filtrada
    query_elementos = f"""
        SELECT DISTINCT ds_elemento
        FROM './despesas-datasets/despesas-2009-2018.parquet'
        WHERE {filtro_estrategico}
        ORDER BY 1
    """
    elementos = con.execute(query_elementos).df()['ds_elemento'].tolist()
    
    print(f"\n📋 LISTA DE ELEMENTOS ({len(elementos)}):")
    print(elementos)

    # 3. Extraímos a lista de AÇÕES filtrada
    query_acoes = f"""
        SELECT DISTINCT ds_acao
        FROM './despesas-datasets/despesas-2009-2018.parquet'
        WHERE {filtro_estrategico}
        ORDER BY 1
    """
    acoes = con.execute(query_acoes).df()['ds_acao'].tolist()

    print(f"\n📋 LISTA DE AÇÕES ({len(acoes)}):")
    print(acoes[:20]) # Mostra só as 20 primeiras no terminal
    
    resto = len(acoes) - 20
    if resto > 0:
        print(f"\n... [E mais {resto} ações não exibidas aqui para não travar o terminal]")

    # 4. Salva as ações em um arquivo de texto
    caminho_arquivo = "dicionario_acoes_filtradas_seguranca.txt"
    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        f.write("=== DICIONÁRIO DE AÇÕES (POLICIAMENTO, DEFESA CIVIL E INTELIGÊNCIA) ===\n\n")
        for acao in acoes:
            f.write(f"{acao}\n")
            
    print(f"\n✅ DICA: O arquivo '{caminho_arquivo}' foi atualizado com a sua lista hiper-focada!")


def auditar_subfuncoes_seguranca():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    print("🔍 Procurando Subfunções exclusivas da Segurança Pública...\n")

    query = """
        SELECT DISTINCT ds_subfuncao_governo
        FROM './despesas-datasets/despesas-2009-2018.parquet'
        WHERE ds_funcao_governo ILIKE '%segurança%' 
           OR ds_funcao_governo ILIKE '%seguranca%'
        ORDER BY 1
    """
    
    subfuncoes = con.execute(query).df()['ds_subfuncao_governo'].tolist()
    
    print(f"🎯 Encontramos {len(subfuncoes)} Subfunções de Segurança cadastradas:")
    for sf in subfuncoes:
        print(f"  - {sf}")


def gerar_dataset_pivotado_despesas(arquivo_origem, arquivo_destino):
    """
    Lê o dataset de despesas original, aplica agregações financeiras baseadas em 
    Funções, Subfunções e Fontes de Recurso, e salva um novo arquivo parquet pivotado.
    """
    print(f"🔄 Iniciando o processamento do dataset...")
    print(f"📂 Origem: {arquivo_origem}")
    print(f"💾 Destino: {arquivo_destino}")

    # Cria a conexão com o DuckDB
    con = duckdb.connect()

    # Define um limite de memória para evitar travamentos
    con.execute("PRAGMA memory_limit='8GB'")
    
    # Se o arquivo de destino já existir, remove para não dar erro
    if os.path.exists(arquivo_destino):
        os.remove(arquivo_destino)

    query = f"""
        COPY (
            SELECT 
                cod_ibge,
                ds_municipio AS city,
                ano_exercicio AS year,
                mes_referencia AS month,
                
                -- ==========================================
                -- 1. FUNÇÕES DO GOVERNO (MACRO-SETORES)
                -- ==========================================
                
                SUM(CASE WHEN UPPER(TRIM(ds_funcao_governo)) = 'SEGURANÇA PÚBLICA' THEN vl_despesa ELSE 0 END) AS gasto_seguranca,
                
                SUM(CASE WHEN UPPER(TRIM(ds_funcao_governo)) IN ('SAÚDE', 'EDUCAÇÃO', 'ASSISTÊNCIA SOCIAL') THEN vl_despesa ELSE 0 END) AS gasto_social_basico,
                
                SUM(CASE WHEN UPPER(TRIM(ds_funcao_governo)) IN ('HABITAÇÃO', 'SANEAMENTO', 'TRANSPORTE', 'URBANISMO') THEN vl_despesa ELSE 0 END) AS gasto_infraestrutura,
                
                SUM(CASE WHEN UPPER(TRIM(ds_funcao_governo)) IN ('ADMINISTRAÇÃO', 'ENCARGOS ESPECIAIS', 'ESSENCIAL À JUSTIÇA', 'JUDICIÁRIA', 'LEGISLATIVA', 'PREVIDÊNCIA SOCIAL', 'RESERVA DE CONTINGÊNCIA') THEN vl_despesa ELSE 0 END) AS gasto_maquina_publica,
                
                SUM(CASE WHEN UPPER(TRIM(ds_funcao_governo)) IN ('AGRICULTURA', 'CIÊNCIA E TECNOLOGIA', 'COMUNICAÇÕES', 'COMÉRCIO E SERVIÇOS', 'ENERGIA', 'INDÚSTRIA', 'ORGANIZAÇÃO AGRÁRIA', 'TRABALHO') THEN vl_despesa ELSE 0 END) AS gasto_desenvolvimento,
                
                SUM(CASE WHEN UPPER(TRIM(ds_funcao_governo)) IN ('CULTURA', 'DEFESA NACIONAL', 'DESPORTO E LAZER', 'DIREITOS DA CIDADANIA', 'GESTÃO AMBIENTAL', 'RELAÇÕES EXTERIORES') THEN vl_despesa ELSE 0 END) AS gasto_outros,

                -- ==========================================
                -- 2. SUBFUNÇÕES DA SEGURANÇA (DIMENSÕES)
                -- ==========================================
                
                SUM(CASE WHEN (ds_funcao_governo ILIKE '%segurança%' OR ds_funcao_governo ILIKE '%seguranca%') 
                          AND UPPER(TRIM(ds_subfuncao_governo)) IN ('DEFESA CIVIL', 'POLICIAMENTO') THEN vl_despesa ELSE 0 END) AS seguranca_operacional,
                
                SUM(CASE WHEN (ds_funcao_governo ILIKE '%segurança%' OR ds_funcao_governo ILIKE '%seguranca%') 
                          AND UPPER(TRIM(ds_subfuncao_governo)) IN ('INFORMAÇÃO E INTELIGÊNCIA', 'TECNOLOGIA DA INFORMAÇÃO', 'TECNOLOGIA DA INFORMATIZAÇÃO') THEN vl_despesa ELSE 0 END) AS seguranca_inteligencia,
                
                SUM(CASE WHEN (ds_funcao_governo ILIKE '%segurança%' OR ds_funcao_governo ILIKE '%seguranca%') 
                          AND UPPER(TRIM(ds_subfuncao_governo)) IN ('ADMINISTRAÇÃO GERAL', 'ALIMENTAÇÃO E NUTRIÇÃO', 'ASSISTÊNCIA COMUNITÁRIA', 'ASSISTÊNCIA À CRIANÇA E AO ADOSLESCENTE', 'COMUNICAÇÃO SOCIAL', 'DEFESA TERRESTRE', 'DIFUSÃO CULTURAL', 'DIREITOS INDIVIDUAIS, COLETIVOS E DIFUSOS', 'EDUCAÇÃO INFANTIL', 'ENSINO FUNDAMENTAL', 'FORMAÇÃO DE RECURSOS HUMANOS', 'INFRA-ESTRUTURA URBANA', 'NORMATIZAÇÃO E FISCALIZAÇÃO', 'PLANEJAMENTO E ORÇAMENTO', 'PREVIDÊNCIA BÁSICA', 'PREVIDÊNCIA DO REGIME ESTATUTÁRIO', 'PROTEÇÃO E BENEFÍCIOS AO TRABALHADOR', 'SERVIÇOS URBNOS', 'TRANSFERÊNCIAS', 'TRANSPORTE RODOVIÁRIO', 'TRANSPORTES COLETIVOS URBANOS', 'TURISMO') THEN vl_despesa ELSE 0 END) AS seguranca_administrativa,

                -- ==========================================
                -- 3. FONTES DE RECURSO (SUPER-BALDES ECONÔMICOS)
                -- ==========================================
                
                SUM(CASE WHEN UPPER(TRIM(ds_fonte_recurso)) IN ('RECURSOS PRÓPRIOS', 'RECURSOS PRÓPRIOS DA ADMINISTRAÇÃO INDIRETA', 'RECURSOS PRÓPRIOS DA ADMINISTRAÇÃO INDIRETA - EXERCICIOS ANTERIORES', 'RECURSOS PRÓPRIOS DA EMPRESA DEPENDENTE', 'RECURSOS PRÓPRIOS DE FUNDOS ESPECIAIS DE DESPESA-VINCULADOS', 'RECURSOS PRÓPRIOS DE FUNDOS ESPECIAIS DE DESPESA-VINCULADOS - EXERCICIOS ANTERIORES', 'TESOURO', 'TESOURO - EXERCICIOS ANTERIORES', 'TESOURO MUNICIPAL', 'TESOURO MUNICIPAL - RECURSOS VINCULADOS') THEN vl_despesa ELSE 0 END) AS fonte_recursos_proprios,
                
                SUM(CASE WHEN UPPER(TRIM(ds_fonte_recurso)) IN ('FUNDO CONSTITUCIONAL DA EDUCAÇÃO', 'TRANSFERÊNCIAS E CONVÊNIOS ESTADUAIS-VINCULADOS', 'TRANSFERÊNCIAS E CONVÊNIOS ESTADUAIS-VINCULADOS - EXERCICIOS ANTERIORES', 'TRANSFERÊNCIAS E CONVÊNIOS FEDERAIS-VINCULADOS', 'TRANSFERÊNCIAS E CONVÊNIOS FEDERAIS-VINCULADOS - EXERCICIOS ANTERIORES', 'TRANSFERÊNCIAS ESTADUAIS', 'TRANSFERÊNCIAS FEDERAIS') THEN vl_despesa ELSE 0 END) AS fonte_transferencias,
                
                SUM(CASE WHEN UPPER(TRIM(ds_fonte_recurso)) IN ('OPERAÇÕES DE CRÉDITO', 'OPERAÇÕES DE CRÉDITO - EXERCICIOS ANTERIORES') THEN vl_despesa ELSE 0 END) AS fonte_operacoes_credito,
                
                SUM(CASE WHEN UPPER(TRIM(ds_fonte_recurso)) IN ('EMENDAS PARLAMENTARES INDIVIDUAIS', 'EMENDAS PARLAMENTARES INDIVIDUAIS - EXERCÍCIOS ANTERIORES') THEN vl_despesa ELSE 0 END) AS fonte_emendas,
                
                SUM(CASE WHEN UPPER(TRIM(ds_fonte_recurso)) IN ('ALIENAÇÃO DE BENS/ATIVOS', 'DEPÓSITOS JUDICIAIS', 'OUTRAS FONTES', 'OUTRAS FONTES DE RECURSOS', 'OUTRAS FONTES DE RECURSOS - EXERCICIOS ANTERIORES', 'RECEITA CONDICIONADA') THEN vl_despesa ELSE 0 END) AS fonte_outras

            FROM '{arquivo_origem}'
            
            -- Remove linhas onde o cod_ibge é nulo para garantir a integridade da chave
            WHERE cod_ibge IS NOT NULL
            
            -- Agrupa por Município, Nome do Município, Ano e Mês (Chave Primária Composta)
            GROUP BY cod_ibge, ds_municipio, ano_exercicio, mes_referencia
            
        ) TO '{arquivo_destino}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """
    
    con.execute(query)
    
    # Validação Básica
    total_linhas = con.execute(f"SELECT count(*) FROM '{arquivo_destino}'").fetchone()[0]
    print(f"✅ Processamento concluído!")
    print(f"📊 O novo dataset gerado tem {total_linhas:,} linhas agregadas.")
    print(f"🚀 O arquivo está pronto em: {arquivo_destino}")



def encontrar_cidade_duplicada():
    con = duckdb.connect()
    
    print("🕵️ Buscando a cidade com dupla identidade no dataset...\n")
    
    query = """
        SELECT 
            cod_ibge, 
            COUNT(DISTINCT ds_municipio) AS qtd_nomes,
            LIST(DISTINCT ds_municipio) AS nomes_utilizados
        FROM './despesas-datasets/despesas-2009-2018.parquet'
        GROUP BY cod_ibge
        HAVING COUNT(DISTINCT ds_municipio) > 1
    """
    
    resultado = con.execute(query).df()
    
    if resultado.empty:
        print("Nenhuma anomalia encontrada.")
    else:
        print("🚨 Encontramos a inconsistência! Veja quem é o culpado:")
        print(resultado)


def padronizar_nomes_ibge(diretorio_parquet, arquivo_csv_ibge):
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")
    
    print("📥 Lendo CSV do IBGE (Lidando com os encodings brasileiros...)")
    
    # Usamos o Pandas para ler o CSV ignorando o problema do UTF-8.
    # O encoding 'latin1' (ou 'iso-8859-1') resolve 99.9% dos arquivos do IBGE/Governo.
    df_ibge = pd.read_csv(arquivo_csv_ibge, sep=';', encoding='latin1')
    
    arquivos = glob.glob(f"{diretorio_parquet}/*.parquet")
    
    print("🧹 Iniciando a padronização oficial dos nomes...")
    
    for arquivo in arquivos:
        # Pula o arquivo pivot para não alterar a tabela final
        if "pivot" in arquivo:
            continue
            
        print(f"Corrigindo ortografia no arquivo: {arquivo}")
        
        arquivo_temp = arquivo.replace(".parquet", "_temp.parquet")
        
        # A mágica do DuckDB: note que fizemos LEFT JOIN direto na variável 'df_ibge' do Pandas!
        query = f"""
            COPY (
                SELECT 
                    t1.* EXCLUDE (ds_municipio),
                    COALESCE(UPPER(TRIM(t2.municipio)), t1.ds_municipio) AS ds_municipio
                FROM '{arquivo}' AS t1
                LEFT JOIN df_ibge AS t2
                ON CAST(t1.cod_ibge AS VARCHAR) = CAST(t2.cod_ibge AS VARCHAR)
            ) TO '{arquivo_temp}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """
        con.execute(query)
        
        os.remove(arquivo)
        os.rename(arquivo_temp, arquivo)
        
    print("\n✅ Todos os datasets base foram atualizados com os nomes oficiais do IBGE!")


def gerar_dataset_mestre():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    arquivo_despesas = './despesas-datasets/despesas-2009-2018-pivot.parquet'
    arquivo_pib = './pib-datasets/pib-2009-2018.parquet' 
    arquivo_crimes = './crime-datasets/crime-2009-2018.parquet' 
    arquivo_destino = './dataset-mestre/crime-despesa-pib-2009-2018.parquet'

    os.makedirs(os.path.dirname(arquivo_destino), exist_ok=True)
    if os.path.exists(arquivo_destino):
        os.remove(arquivo_destino)

    query = f"""
        COPY (
            SELECT 
                -- 1. IDENTIFICADORES E TEMPO
                d.cod_ibge,
                d.city,
                d.year,
                d.month,
                
                -- 2. DESPESAS PÚBLICAS
                d.gasto_seguranca,
                d.gasto_social_basico,
                d.gasto_infraestrutura,
                d.gasto_maquina_publica,
                d.gasto_desenvolvimento,
                d.gasto_outros,
                -- Detalhamento de Segurança
                d.seguranca_operacional,
                d.seguranca_inteligencia,
                d.seguranca_administrativa,
                -- Detalhamento de Fontes de Recurso
                d.fonte_recursos_proprios,
                d.fonte_transferencias,
                d.fonte_operacoes_credito,
                d.fonte_emendas,
                d.fonte_outras,
                
                -- 3. ECONOMIA E PIB
                p.pib_total,
                p.pib_per_capita,
                p.agropecuaria AS pib_agropecuaria,
                p.industria AS pib_industria,
                p.servicos AS pib_servicos,
                p.adm_publica AS pib_adm_publica,
                p.impostos AS pib_impostos_liquidos,

                -- 4. CRIMES (Buckets Agregados)
                (COALESCE(c.total_de_roubo_outros, 0) + COALESCE(c.roubo_de_veiculo, 0) + 
                 COALESCE(c.furto_outros, 0) + COALESCE(c.furto_de_veiculo, 0)) AS crimes_patrimonio,
                
                (COALESCE(c.homicidio_doloso, 0) + COALESCE(c.latrocinio, 0) + 
                 COALESCE(c.lesao_corporal_seguida_de_morte, 0) + COALESCE(c.tentativa_de_homicidio, 0)) AS crimes_violentos_vida,
                
                (COALESCE(c.total_de_estupro, 0)) AS crimes_dignidade_sexual,
                
                (COALESCE(c.homicidio_culposo_outros, 0) + COALESCE(c.homicidio_culposo_por_acidente_de_transito, 0) +
                 COALESCE(c.lesao_corporal_culposa_outras, 0) + COALESCE(c.lesao_corporal_culposa_por_acidente_de_transito, 0)) AS crimes_transito_e_culposos,
                
                (COALESCE(c.lesao_corporal_dolosa, 0)) AS crimes_outros_violentos

            FROM '{arquivo_despesas}' AS d
            
            LEFT JOIN '{arquivo_pib}' AS p 
                ON CAST(d.cod_ibge AS VARCHAR) = CAST(p.cod_ibge AS VARCHAR) 
                AND d.year = p.ano_exercicio
                
            LEFT JOIN '{arquivo_crimes}' AS c 
                ON CAST(d.cod_ibge AS VARCHAR) = CAST(c.cod_ibge AS VARCHAR) 
                AND d.year = c.year
                AND d.month = c.month
                
        ) TO '{arquivo_destino}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """
    
    con.execute(query)
    
    total_linhas = con.execute(f"SELECT count(*) FROM '{arquivo_destino}'").fetchone()[0]
    print(f"Finalizada com {total_linhas:,} linhas!")
    print(f"Arquivo pronto em: {arquivo_destino}")

def convert_gdv_to_parquet_with_ibge(domain='gdvDespesasExcel'):
    print("Iniciando processo de conversão e enriquecimento com IBGE...")
    
    # 1. Configurando o DuckDB
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")
    
    # 2. Carrega a base do IBGE e define as correções UMA VEZ (fora do loop para ser rápido)
    print("Carregando base de municípios do IBGE...")
    df_ibge = pd.read_csv('codigos_municipios_regioes.csv', sep=';', encoding='iso-8859-1')
    
    correcoes_municipios = {
        'ARCO IRIS': 'ARCO-IRIS',
        'BIRITIBA-MIRIM': 'BIRITIBA MIRIM',
        'BRODOSQUI': 'BRODOWSKI',
        'EMBU': 'EMBU DAS ARTES',
        'EMBU GUACU': 'EMBU-GUACU',
        'IPAUCU': 'IPAUSSU',
        'MOGI-GUACU': 'MOGI GUACU',
        'MOGI-MIRIM': 'MOGI MIRIM',
        'NOVA LUSITANIA': 'NOVA LUZITANIA',
        'PALMEIRA D_OESTE': "PALMEIRA D'OESTE",
        'SALMORAO': 'SALMOURAO',
        'SEVERINEA': 'SEVERINIA',
        'SUD MENUCCI': 'SUD MENNUCCI',
        'SUZANOPOLIS': 'SUZANAPOLIS'
    }

    # 3. Loop pelos anos
    for year in range(2009, 2021):
        csv_file = f'./{domain}-datasets/{domain}-{year}.csv'
        parquet_file = f'./{domain}-datasets/{domain}-{year}_com_ibge.parquet'

        if os.path.exists(parquet_file):
            print(f"\n--- {parquet_file} já existe. Pulando conversão do ano {year}... ---")
            continue

        if os.path.exists(csv_file):
            print(f"\nCarregando e processando {csv_file}...")
            start_time = time.time()

            # Lendo o CSV com Pandas para tipar números brasileiros corretamente
            df = pd.read_csv(
                csv_file, 
                encoding='iso-8859-1', 
                sep=',', 
                low_memory=False,
                decimal=',',
                thousands='.',
                usecols=lambda c: not c.startswith('Unnamed:')
            )
            
            # Limpeza da coluna Município
            if 'Município' in df.columns:
                # Usa .str[-1] para pegar sempre a última parte do split, com ou sem o prefixo
                df['Município'] = df['Município'].astype(str).str.split(' - ', n=1).str[-1].str.strip()
                df['Município'] = df['Município'].replace(correcoes_municipios)
            
            # Cruzamento e Exportação direta via DuckDB
            query_exportacao = f"""
                COPY (
                    SELECT 
                        despesas.*,
                        ibge.cod_ibge,
                        ibge.drs,
                        ibge.r_saude
                    FROM df AS despesas
                    INNER JOIN df_ibge AS ibge 
                        ON UPPER(strip_accents(TRIM(despesas.Município))) = UPPER(strip_accents(TRIM(ibge.municipio)))
                ) TO '{parquet_file}' (FORMAT PARQUET);
            """
            
            con.execute(query_exportacao)
            
            # Cálculos de performance e log de sucesso
            elapsed_time = time.time() - start_time
            parquet_size_mb = os.path.getsize(parquet_file) / (1024 * 1024)
            
            print(f"SUCESSO: Salvo {parquet_size_mb:.2f} MB em {parquet_file}")
            print(f"Tempo levado: {elapsed_time:.2f} segundos")

        else:
            print(f"\nArquivo {csv_file} não encontrado. Pulando...")

def gerar_master_parquet_via_sql():
    start_time = time.time()
    print("Iniciando processamento analítico nativo no DuckDB (SQL)...")
    
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")
    
    arquivo_destino = './gdvDespesasExcel-datasets/master_despesas_municipios_2010_2019.parquet'
    os.makedirs(os.path.dirname(arquivo_destino), exist_ok=True)

    query = f"""
        COPY (
            WITH dados_brutos AS (
                SELECT 
                    CAST(regexp_extract(filename, 'gdvDespesasExcel-(\d+)', 1) AS INTEGER) AS Ano,
                    cod_ibge,
                    "Município",
                    (COALESCE("Pago", 0) + COALESCE("Pago Restos", 0)) AS pago_total,
                    
                    -- 1. BUCKET: ÓRGÃO (Administrativo)
                    CASE 
                        WHEN UPPER("Órgão") LIKE '%SEGURANCA%' OR UPPER("Órgão") LIKE '%SEGURANÇA%' THEN 'seguranca'
                        WHEN UPPER("Órgão") LIKE '%SAUDE%' OR UPPER("Órgão") LIKE '%SAÚDE%' THEN 'saude'
                        WHEN UPPER("Órgão") LIKE '%EDUCACAO%' OR UPPER("Órgão") LIKE '%EDUCAÇÃO%' THEN 'educacao'
                        WHEN UPPER("Órgão") LIKE '%ADMINISTRACAO%' OR UPPER("Órgão") LIKE '%JUSTICA%' OR UPPER("Órgão") LIKE '%TRIBUNAL%' THEN 'maquina'
                        ELSE 'outros'
                    END AS b_orgao,

                    -- 2. BUCKET: FUNÇÃO (Área Governamental)
                    CASE 
                        WHEN UPPER("Função") LIKE '%SEGURANCA%' OR UPPER("Função") LIKE '%SEGURANÇA%' THEN 'seguranca'
                        WHEN UPPER("Função") LIKE '%SAUDE%' OR UPPER("Função") LIKE '%SAÚDE%' THEN 'saude'
                        WHEN UPPER("Função") LIKE '%EDUCACAO%' OR UPPER("Função") LIKE '%EDUCAÇÃO%' THEN 'educacao'
                        WHEN UPPER("Função") IN ('01 - LEGISLATIVA', '02 - JUDICIARIA', '03 - ESSENCIAL A JUSTICA', '04 - ADMINISTRACAO') THEN 'maquina'
                        ELSE 'outros'
                    END AS b_funcao,
                    
                    -- 3. BUCKET: SUBFUNÇÃO (Atrelada à Função de Segurança)
                    CASE 
                        WHEN UPPER("Função") LIKE '%SEGURANCA%' OR UPPER("Função") LIKE '%SEGURANÇA%' THEN
                            CASE 
                                WHEN "Sub Função" LIKE '%181%' THEN 'policiamento'
                                WHEN "Sub Função" LIKE '%182%' THEN 'defesa_civil'
                                WHEN "Sub Função" LIKE '%183%' THEN 'inteligencia'
                                WHEN "Sub Função" LIKE '%122%' THEN 'adm_geral'
                                ELSE 'outras'
                            END
                        ELSE NULL
                    END AS b_subfuncao,
                    
                    -- 4. BUCKET: PROGRAMA (Estratégico - Filtrado por Segurança)
                    CASE 
                        WHEN UPPER("Função") LIKE '%SEGURANCA%' OR UPPER("Função") LIKE '%SEGURANÇA%' THEN
                            CASE 
                                WHEN REGEXP_MATCHES(UPPER("Programa"), 'POLICI|OSTENSIV|PATRULH|FORCA|FORÇA|SEGURAN') THEN 'prog_operacional'
                                WHEN REGEXP_MATCHES(UPPER("Programa"), 'REFORMA|PREDIO|VIATURA|EQUIPAMENTO|CONSTRUCAO|INFRA') THEN 'prog_infraestrutura'
                                ELSE 'prog_adm_suporte'
                            END
                        ELSE NULL
                    END AS b_programa,

                    -- 5. BUCKET: AÇÃO (Tático/Execução - Filtrado por Segurança)
                    CASE 
                        WHEN UPPER("Função") LIKE '%SEGURANCA%' OR UPPER("Função") LIKE '%SEGURANÇA%' THEN
                            CASE 
                                WHEN REGEXP_MATCHES(UPPER("Ação"), 'POLICI|OSTENSIV|PATRULH|FORCA|FORÇA') THEN 'acao_operacional'
                                WHEN REGEXP_MATCHES(UPPER("Ação"), 'INTELIG|INFORMA|TECNOL|SISTEMA|MONITORAM') THEN 'acao_inteligencia'
                                WHEN REGEXP_MATCHES(UPPER("Ação"), 'REFORMA|PREDIO|VIATURA|EQUIPAMENTO|CONSTRUCAO') THEN 'acao_infraestrutura'
                                ELSE 'acao_adm_suporte'
                            END
                        ELSE NULL
                    END AS b_acao,
                    
                    -- 6. BUCKET: TIPO DE DESPESA
                    CASE 
                        WHEN REGEXP_MATCHES(UPPER("Despesa"), 'PESSOAL|ENCARGOS|PROVENTOS') THEN 'pessoal'
                        WHEN REGEXP_MATCHES(UPPER("Despesa"), 'MATERIAL|CONSUMO') THEN 'materiais'
                        WHEN REGEXP_MATCHES(UPPER("Despesa"), 'SERVICO|SERVIÇO') THEN 'servicos'
                        WHEN REGEXP_MATCHES(UPPER("Despesa"), 'INVESTIMENTO|OBRAS|EQUIPAMENTO') THEN 'investimentos'
                        ELSE 'outras'
                    END AS b_despesa
                    
                FROM read_parquet('./gdvDespesasExcel-datasets/gdvDespesasExcel-*_com_ibge.parquet', filename=true)
            )
            
            -- AGREGAÇÃO CONDICIONAL (SUPER PIVOT)
            SELECT 
                Ano,
                cod_ibge,
                "Município" AS city,
                
                -- Pivot: Órgãos
                SUM(CASE WHEN b_orgao = 'seguranca' THEN pago_total ELSE 0 END) AS orgao_seguranca,
                SUM(CASE WHEN b_orgao = 'saude' THEN pago_total ELSE 0 END) AS orgao_saude,
                SUM(CASE WHEN b_orgao = 'educacao' THEN pago_total ELSE 0 END) AS orgao_educacao,
                SUM(CASE WHEN b_orgao = 'maquina' THEN pago_total ELSE 0 END) AS orgao_maquina,
                
                -- Pivot: Funções
                SUM(CASE WHEN b_funcao = 'seguranca' THEN pago_total ELSE 0 END) AS funcao_seguranca,
                SUM(CASE WHEN b_funcao = 'saude' THEN pago_total ELSE 0 END) AS funcao_saude,
                SUM(CASE WHEN b_funcao = 'educacao' THEN pago_total ELSE 0 END) AS funcao_educacao,
                SUM(CASE WHEN b_funcao = 'maquina' THEN pago_total ELSE 0 END) AS funcao_maquina,
                
                -- Porcentagens de Segurança
                (SUM(CASE WHEN b_orgao = 'seguranca' THEN pago_total ELSE 0 END) / NULLIF(SUM(pago_total), 0)) * 100 AS pct_orgao_seguranca,
                (SUM(CASE WHEN b_funcao = 'seguranca' THEN pago_total ELSE 0 END) / NULLIF(SUM(pago_total), 0)) * 100 AS pct_funcao_seguranca,
                
                -- Porcentagens do Resto (Tudo que NÃO é segurança)
                (SUM(CASE WHEN b_orgao != 'seguranca' THEN pago_total ELSE 0 END) / NULLIF(SUM(pago_total), 0)) * 100 AS pct_orgao_resto,
                (SUM(CASE WHEN b_funcao != 'seguranca' THEN pago_total ELSE 0 END) / NULLIF(SUM(pago_total), 0)) * 100 AS pct_funcao_resto,
                
                -- Pivot: Tipo de Despesa
                SUM(CASE WHEN b_despesa = 'pessoal' THEN pago_total ELSE 0 END) AS desp_pessoal_encargos,
                SUM(CASE WHEN b_despesa = 'materiais' THEN pago_total ELSE 0 END) AS desp_materiais,
                SUM(CASE WHEN b_despesa = 'servicos' THEN pago_total ELSE 0 END) AS desp_servicos_terceiros,
                SUM(CASE WHEN b_despesa = 'investimentos' THEN pago_total ELSE 0 END) AS desp_investimentos_obras,
                
                -- Pivot: Subfunções de Segurança
                SUM(CASE WHEN b_subfuncao = 'policiamento' THEN pago_total ELSE 0 END) AS seg_sub_policiamento,
                SUM(CASE WHEN b_subfuncao = 'defesa_civil' THEN pago_total ELSE 0 END) AS seg_sub_defesa_civil,
                SUM(CASE WHEN b_subfuncao = 'inteligencia' THEN pago_total ELSE 0 END) AS seg_sub_inteligencia,
                SUM(CASE WHEN b_subfuncao = 'adm_geral' THEN pago_total ELSE 0 END) AS seg_sub_adm_geral,
                
                -- Pivot: PROGRAMAS de Segurança
                SUM(CASE WHEN b_programa = 'prog_operacional' THEN pago_total ELSE 0 END) AS seg_prog_operacional,
                SUM(CASE WHEN b_programa = 'prog_infraestrutura' THEN pago_total ELSE 0 END) AS seg_prog_infraestrutura,
                SUM(CASE WHEN b_programa = 'prog_adm_suporte' THEN pago_total ELSE 0 END) AS seg_prog_adm_suporte,

                -- Pivot: AÇÕES de Segurança
                SUM(CASE WHEN b_acao = 'acao_operacional' THEN pago_total ELSE 0 END) AS seg_acao_operacional,
                SUM(CASE WHEN b_acao = 'acao_inteligencia' THEN pago_total ELSE 0 END) AS seg_acao_inteligencia,
                SUM(CASE WHEN b_acao = 'acao_infraestrutura' THEN pago_total ELSE 0 END) AS seg_acao_infraestrutura,
                SUM(CASE WHEN b_acao = 'acao_adm_suporte' THEN pago_total ELSE 0 END) AS seg_acao_adm_suporte
                
            FROM dados_brutos
            GROUP BY Ano, cod_ibge, "Município"
            ORDER BY Ano, cod_ibge
        ) TO '{arquivo_destino}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    con.execute(query)
    
    elapsed_time = time.time() - start_time
    total_linhas = con.execute(f"SELECT count(*) FROM '{arquivo_destino}'").fetchone()[0]
    
    print("\n" + "="*60)
    print("🚀 MASTER PARQUET (COM METRICAS DE RESTO) GERADO! 🚀")
    print("="*60)
    print(f"Linhas consoladas: {total_linhas:,}")
    print(f"Tempo de execução: {elapsed_time:.2f} segundos")
    print("="*60)


def gerar_master_dataset_final():
    start_time = time.time()
    print("Iniciando a junção final dos 3 domínios (Despesas, PIB e Crimes) com Features Avançadas...")
    
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    # Caminhos baseados na sua infraestrutura de pastas
    arquivo_despesas = './gdvDespesasExcel-datasets/master_despesas_municipios_2010_2019.parquet'
    arquivo_pib = './pib-datasets/pib-2010-2019.parquet' 
    arquivo_crimes = './crime-datasets/crime-2010-2019.parquet' 
    arquivo_destino = './dataset-mestre/master_dataset_2010_2019.parquet'

    # Garante a existência do diretório e limpa arquivos antigos
    os.makedirs(os.path.dirname(arquivo_destino), exist_ok=True)
    if os.path.exists(arquivo_destino):
        os.remove(arquivo_destino)

    query = f"""
        COPY (
            -- 1. CTE: Agrupamento Anual do dataset de crimes mantendo a região geográfica
            WITH annual_crimes AS (
                SELECT 
                    cod_ibge,
                    year,
                    region,
                    SUM(COALESCE(total_de_roubo_outros, 0) + COALESCE(roubo_de_veiculo, 0) + 
                        COALESCE(furto_outros, 0) + COALESCE(furto_de_veiculo, 0)) AS crimes_patrimonio,
                    
                    SUM(COALESCE(homicidio_doloso, 0) + COALESCE(latrocinio, 0) + 
                        COALESCE(lesao_corporal_seguida_de_morte, 0) + COALESCE(tentativa_de_homicidio, 0)) AS crimes_violentos_vida,
                    
                    SUM(COALESCE(total_de_estupro, 0)) AS crimes_dignidade_sexual,
                    
                    SUM(COALESCE(homicidio_culposo_outros, 0) + COALESCE(homicidio_culposo_por_acidente_de_transito, 0) +
                        COALESCE(lesao_corporal_culposa_outras, 0) + COALESCE(lesao_corporal_culposa_por_acidente_de_transito, 0)) AS crimes_transito_e_culposos,
                    
                    SUM(COALESCE(lesao_corporal_dolosa, 0)) AS crimes_outros_violentos
                FROM '{arquivo_crimes}'
                GROUP BY cod_ibge, year, region
            )
            
            -- 2. Seleção Consolidada com todas as features financeiras limpas e separadas
            SELECT 
                -- Chaves Primárias e Identificadores (Padronizando para 'year' na saída)
                d.Ano AS year,
                d.cod_ibge,
                d.city,
                c.region,
                
                -- =========================================================
                -- DOMÍNIO 1: DESPESAS PÚBLICAS (FEATURES PIVOTADAS NO SCRIPT SQL)
                -- =========================================================
                -- Valores Absolutos por ÓRGÃO (A Estrutura que gastou)
                d.orgao_seguranca,
                d.orgao_saude,
                d.orgao_educacao,
                d.orgao_maquina,
                
                -- Valores Absolutos por FUNÇÃO (A Área onde o dinheiro foi aplicado)
                d.funcao_seguranca,
                d.funcao_saude,
                d.funcao_educacao,
                d.funcao_maquina,
                
                -- Distribuição Orçamentária em Porcentagem (%)
                d.pct_orgao_seguranca,
                d.pct_funcao_seguranca,
                d.pct_orgao_resto,
                d.pct_funcao_resto,
                
                -- Elementos de Gasto (Categorias Econômicas de Despesa)
                d.desp_pessoal_encargos,
                d.desp_materiais,
                d.desp_servicos_terceiros,
                d.desp_investimentos_obras,
                
                -- Subfunções da Área de Segurança Pública
                d.seg_sub_policiamento,
                d.seg_sub_defesa_civil,
                d.seg_sub_inteligencia,
                d.seg_sub_adm_geral,
                
                -- PROGRAMAS de Segurança (Nível Estratégico)
                d.seg_prog_operacional,
                d.seg_prog_infraestrutura,
                d.seg_prog_adm_suporte,

                -- AÇÕES de Segurança (Nível Tático/Execução)
                d.seg_acao_operacional,
                d.seg_acao_inteligencia,
                d.seg_acao_infraestrutura,
                d.seg_acao_adm_suporte,
                
                -- =========================================================
                -- DOMÍNIO 2: ECONOMIA (DADOS ANUAIS DO PIB)
                -- =========================================================
                p.pib_total,
                p.pib_per_capita,
                p.agropecuaria AS pib_agropecuaria,
                p.industria AS pib_industria,
                p.servicos AS pib_servicos,
                p.adm_publica AS pib_adm_publica,
                p.impostos AS pib_impostos_liquidos,

                -- =========================================================
                -- DOMÍNIO 3: SEGURANÇA (ÍNDICES CRIMINAIS ANUAIS AGREGADOS)
                -- =========================================================
                c.crimes_patrimonio,
                c.crimes_violentos_vida,
                c.crimes_dignidade_sexual,
                c.crimes_transito_e_culposos,
                c.crimes_outros_violentos

            FROM '{arquivo_despesas}' AS d
            
            -- Cruzamento com PIB: d.Ano (Despesas) bate com p.ano_exercicio (PIB)
            LEFT JOIN '{arquivo_pib}' AS p 
                ON CAST(d.cod_ibge AS VARCHAR) = CAST(p.cod_ibge AS VARCHAR) 
                AND d.Ano = p.ano_exercicio
                
            -- Cruzamento com Crimes: d.Ano (Despesas) bate com c.year (calculado na CTE de crimes)
            LEFT JOIN annual_crimes AS c 
                ON CAST(d.cod_ibge AS VARCHAR) = CAST(c.cod_ibge AS VARCHAR) 
                AND d.Ano = c.year
                
        ) TO '{arquivo_destino}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """
    
    con.execute(query)
    
    elapsed_time = time.time() - start_time
    total_linhas = con.execute(f"SELECT count(*) FROM '{arquivo_destino}'").fetchone()[0]
    
    print("\n" + "="*65)
    print("🚀 DATASET MASTER FINAL CONCLUÍDO E SEPARADO POR HIERARQUIAS! 🚀")
    print("="*65)
    print(f"Total de Linhas Consolidadas: {total_linhas:,}")
    print(f"Tempo de Execução do Super-Join: {elapsed_time:.2f} segundos")
    print(f"Destino Salvo com ZSTD: {arquivo_destino}")
    print("="*65)

if __name__ == "__main__":

    describe_dataset_columns()
    print("------------------------------------", "\n")
    cardinalidade_despesas()
    print("------------------------------------", "\n")
    extrair_dicionario_dados()
    print("------------------------------------", "\n")
    #gerar_master_parquet_via_sql()
    gerar_master_dataset_final()



