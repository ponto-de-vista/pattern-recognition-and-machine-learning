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
        './despesas-datasets/despesas-2009-2018-pivot.parquet', 
        './pib-datasets/pib-2009-2018.parquet', 
        './crime-datasets/crime-2009-2018.parquet'
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

    query = """
        SELECT 
            COUNT(DISTINCT UPPER(TRIM(ds_funcao_governo))) AS qtd_funcoes_limpas,
            COUNT(DISTINCT UPPER(TRIM(ds_subfuncao_governo))) AS qtd_subfuncoes_limpas,
            COUNT(DISTINCT UPPER(TRIM(ds_programa))) AS qtd_programas_limpos,
            COUNT(DISTINCT UPPER(TRIM(ds_acao))) AS qtd_acoes_limpas,
            COUNT(DISTINCT UPPER(TRIM(ds_fonte_recurso))) AS qtd_fontes_limpas,
            COUNT(DISTINCT UPPER(TRIM(ds_elemento))) AS qtd_elementos_limpos
        FROM './despesas-datasets/despesas-2009-2018.parquet'
    """

    resultado = con.execute(query).df()
    print(resultado.T)


def extrair_dicionario_dados():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")

    print("Extraindo dicionário de dados limpo e filtrado...\n")

    def pegar_lista(coluna, filtro_extra=""):
        query = f"""
            SELECT DISTINCT UPPER(TRIM({coluna})) AS categoria_limpa
            FROM './despesas-datasets/despesas-2009-2018.parquet' 
            WHERE {coluna} IS NOT NULL {filtro_extra}
            ORDER BY 1
        """
        return con.execute(query).df()['categoria_limpa'].tolist()

    # 1. Funções globais
    funcoes = pegar_lista("ds_funcao_governo")
    
    # 2. Fontes globais
    fontes = pegar_lista("ds_fonte_recurso")

    # 3. Subfunções (Filtro 1: Apenas Segurança Pública)
    trava_seguranca = "AND (ds_funcao_governo ILIKE '%segurança%' OR ds_funcao_governo ILIKE '%seguranca%')"
    subfuncoes_seguranca = pegar_lista("ds_subfuncao_governo", trava_seguranca)

    # --- ÁREA DE IMPRESSÃO ---
    print(f"🔹 FUNÇÕES REAIS ({len(funcoes)}):")
    print(funcoes, "\n")

    print(f"🔹 FONTES DE RECURSO REAIS ({len(fontes)}):")
    print(fontes, "\n")

    print(f"🔹 SUBFUNÇÕES REAIS (Filtradas para Segurança Pública) ({len(subfuncoes_seguranca)}):")
    for sf in subfuncoes_seguranca:
        print(f"  - {sf}")
    print("\n")


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
                -- Identificadores e Tempo
                d.cod_ibge,
                d.city,
                d.year,
                d.month,
                
                -- 1. DESPESAS PÚBLICAS (Buckets Agregados)
                d.gasto_seguranca,
                d.gasto_social_basico,
                d.gasto_infraestrutura,
                d.gasto_maquina_publica,
                d.gasto_desenvolvimento,
                d.gasto_outros,
                
                -- 2. ECONOMIA E PIB (Identificação por prefixo pib_)
                p.pib_total,
                p.pib_per_capita,
                p.agropecuaria AS pib_agropecuaria,
                p.industria AS pib_industria,
                p.servicos AS pib_servicos,
                p.adm_publica AS pib_adm_publica,
                p.impostos AS pib_impostos_liquidos,

                -- 3. CRIMES (Buckets de Ocorrências Mensais - Regras SSP-SP)
                (COALESCE(c.total_de_roubo_outros, 0) + COALESCE(c.roubo_de_veiculo, 0) + 
                 COALESCE(c.furto_outros, 0) + COALESCE(c.furto_de_veiculo, 0)) AS crimes_patrimonio,
                
                (COALESCE(c.homicidio_doloso, 0) + COALESCE(c.latrocinio, 0) + 
                 COALESCE(c.lesao_corporal_seguida_de_morte, 0) + COALESCE(c.tentativa_de_homicidio, 0)) AS crimes_violentos_vida,
                
                (COALESCE(c.total_de_estupro, 0)) AS crimes_dignidade_sexual,
                
                (COALESCE(c.homicidio_culposo_outros, 0) + COALESCE(c.homicidio_culposo_por_acidente_de_transito, 0) +
                 COALESCE(c.lesao_corporal_culposa_outras, 0) + COALESCE(c.lesao_corporal_culposa_por_acidente_de_transito, 0)) AS crimes_transito_e_culposos,
                
                (COALESCE(c.lesao_corporal_dolosa, 0)) AS crimes_outros_violentos

            FROM '{arquivo_despesas}' AS d
            
            -- Join Anual do PIB (Chaves: IBGE + Ano)
            LEFT JOIN '{arquivo_pib}' AS p 
                ON CAST(d.cod_ibge AS VARCHAR) = CAST(p.cod_ibge AS VARCHAR) 
                AND d.year = p.ano_exercicio
                
            -- Join Mensal dos Crimes (Chaves: IBGE + Ano + Mês)
            LEFT JOIN '{arquivo_crimes}' AS c 
                ON CAST(d.cod_ibge AS VARCHAR) = CAST(c.cod_ibge AS VARCHAR) 
                AND d.year = c.year
                AND d.month = c.month
                
        ) TO '{arquivo_destino}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """
    
    con.execute(query)
    
    total_linhas = con.execute(f"SELECT count(*) FROM '{arquivo_destino}'").fetchone()[0]
    print(f"Finalizada com {total_linhas:,} linhas!")
    print(f"Arquivo mestre disponível em: {arquivo_destino}")


if __name__ == "__main__":

    describe_dataset_columns()
    cardinalidade_despesas()
    extrair_dicionario_dados()
    
