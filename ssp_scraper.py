"""
SSP-SP Criminal Statistics Scraper
====================================
Scrapes monthly criminal data from https://www.ssp.sp.gov.br/estatistica/dados-mensais
for years 2009-2018, all regions and their cities, storing results in DuckDB.

Speed strategy
--------------
The bottleneck is browser interaction (selecting dropdowns + waiting for Angular
to re-render). We parallelise at the YEAR level: each year gets its own Chrome
instance running in a separate thread via ThreadPoolExecutor. All threads write
to the same DuckDB file through a shared threading.Lock so writes don't collide.

Set MAX_WORKERS to the number of parallel browsers you want. A good starting
point is 3-5; beyond that the SSP server may start rate-limiting you.

Dependencies:
    pip install selenium webdriver-manager duckdb pandas
"""

import time
import threading
import unicodedata
import duckdb
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
URL            = "https://www.ssp.sp.gov.br/estatistica/dados-mensais"
YEARS          = list(range(2009, 2019))   # 2009 to 2018 inclusive
DB_PATH        = "ssp_criminal.duckdb"
TABLE_NAME     = "criminal_stats"
IBGE_CSV_PATH  = "codigos_municipios_regioes.csv"
WAIT_TIMEOUT   = 15                        # seconds to wait for each element
MAX_WORKERS    = 4                         # parallel Chrome instances (one per year)

# Shared state for parallel workers
_db_lock         = threading.Lock()        # serialises all DuckDB writes
_unmatched_lock  = threading.Lock()
_unmatched_cities: set = set()


# ---------------------------------------------------------------------------
# HELPER -- normalise strings for fuzzy city-name matching
# ---------------------------------------------------------------------------
def normalize(text: str) -> str:
    """
    Strips accents, uppercases and collapses whitespace so that
    'Sao Paulo', 'SAO PAULO' and 'Sao Paulo' all map to 'SAO PAULO'.
    """
    if not isinstance(text, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_str.upper().split())


# ---------------------------------------------------------------------------
# STEP 1 -- Load IBGE lookup from CSV
# ---------------------------------------------------------------------------
def load_ibge_lookup(csv_path: str) -> dict:
    """
    Returns {normalised_city_name: cod_ibge, ...}.
    Rows with very short cod_ibge (state-level rows) are dropped.
    """
    df = pd.read_csv(csv_path, encoding="latin1", sep=None, engine="python")
    df = df[["cod_ibge", "municipio"]].dropna()
    df = df[df["cod_ibge"].astype(str).str.len() > 4]
    lookup = {
        normalize(row["municipio"]): int(row["cod_ibge"])
        for _, row in df.iterrows()
    }
    print(f"[init] IBGE lookup loaded: {len(lookup)} cities.")
    return lookup


def get_cod_ibge(city_name: str, lookup: dict):
    return lookup.get(normalize(city_name))


# ---------------------------------------------------------------------------
# STEP 2 -- Create a headless Chrome driver
# ---------------------------------------------------------------------------
def create_driver() -> webdriver.Chrome:
    """
    Each worker thread calls this to get its own isolated Chrome instance.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=options)
    driver.get(URL)
    return driver


# ---------------------------------------------------------------------------
# STEP 3 -- Select the "Criminal" radio button
# ---------------------------------------------------------------------------
def select_criminal_radio(driver: webdriver.Chrome) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "input[type='radio']")))
    for label in driver.find_elements(By.CSS_SELECTOR, "label.radio-inline"):
        if "Criminal" in label.text and "Produtividade" not in label.text:
            driver.execute_script("arguments[0].click();", label.find_element(By.TAG_NAME, "input"))
            return
    raise RuntimeError("Could not find the 'Criminal' radio button.")


# ---------------------------------------------------------------------------
# STEP 4 -- Select a year
# ---------------------------------------------------------------------------
def select_year(driver: webdriver.Chrome, year: int) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalAno']")
    ))
    Select(el).select_by_value(str(year))
    time.sleep(0.5)


# ---------------------------------------------------------------------------
# STEP 5 -- Get all regions
# ---------------------------------------------------------------------------
def get_regions(driver: webdriver.Chrome) -> list:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalRegiao']")
    ))
    return [
        {"value": opt.get_attribute("value"), "name": opt.text.strip()}
        for opt in Select(el).options
        if opt.get_attribute("value") != "0"
    ]


# ---------------------------------------------------------------------------
# STEP 6 -- Select a region
# ---------------------------------------------------------------------------
def select_region(driver: webdriver.Chrome, region_value: str, region_name: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalRegiao']")
    ))
    Select(el).select_by_value(region_value)
    time.sleep(1)


# ---------------------------------------------------------------------------
# STEP 7 -- Get all cities for the current region
# ---------------------------------------------------------------------------
def get_cities(driver: webdriver.Chrome) -> list:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalMunicipio']")
    ))
    return [
        {"value": opt.get_attribute("value"), "name": opt.text.strip()}
        for opt in Select(el).options
        if opt.get_attribute("value") not in ("0", "")
    ]


# ---------------------------------------------------------------------------
# STEP 8 -- Select a city
# ---------------------------------------------------------------------------
def select_city(driver: webdriver.Chrome, city_value: str, city_name: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalMunicipio']")
    ))
    Select(el).select_by_value(city_value)
    time.sleep(2)


# ---------------------------------------------------------------------------
# STEP 9 -- Scrape the accordion table (Total column only)
# ---------------------------------------------------------------------------
def scrape_table(
    driver:   webdriver.Chrome,
    year:     int,
    region:   str,
    city:     str,
    cod_ibge,
) -> list:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".accordion-body table")))
    except Exception:
        print(f"  [year={year}] No table for {city}. Skipping.")
        return []

    rows_data = []
    for table in driver.find_elements(By.CSS_SELECTOR, ".accordion-body table"):
        headers = [th.text.strip() for th in table.find_elements(By.CSS_SELECTOR, "thead th")]
        if "Total" not in headers:
            continue
        total_idx = headers.index("Total")

        for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
            th_cells = row.find_elements(By.TAG_NAME, "th")
            td_cells = row.find_elements(By.TAG_NAME, "td")
            if not th_cells:
                continue
            crime_type   = th_cells[0].text.strip()
            td_total_idx = total_idx - 1
            if td_total_idx < 0 or td_total_idx >= len(td_cells):
                continue
            raw = td_cells[td_total_idx].text.strip().replace(".", "").replace(",", "")
            try:
                total_value = int(raw)
            except ValueError:
                total_value = None
            rows_data.append({
                "year":       year,
                "region":     region,
                "city":       city,
                "cod_ibge":   cod_ibge,
                "crime_type": crime_type,
                "total":      total_value,
            })
    return rows_data


# ---------------------------------------------------------------------------
# STEP 10 -- Set up DuckDB (drops and recreates the table on every run)
# ---------------------------------------------------------------------------
def setup_database(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Connects to DuckDB, drops the table if it exists, and creates it fresh.
    Called once in the main thread before workers start.
    """
    con = duckdb.connect(db_path)
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            year       INTEGER  NOT NULL,
            region     VARCHAR  NOT NULL,
            city       VARCHAR  NOT NULL,
            cod_ibge   INTEGER,
            crime_type VARCHAR  NOT NULL,
            total      INTEGER,
            PRIMARY KEY (year, city, crime_type)
        )
    """)
    print(f"[init] Table '{TABLE_NAME}' reset and ready in '{db_path}'.")
    return con


# ---------------------------------------------------------------------------
# STEP 11 -- Insert rows into DuckDB (thread-safe via lock)
# ---------------------------------------------------------------------------
def insert_rows(con: duckdb.DuckDBPyConnection, rows: list) -> None:
    """
    Inserts a batch of rows.  The caller must hold _db_lock while calling this.
    """
    if not rows:
        return
    df = pd.DataFrame(rows, columns=["year", "region", "city", "cod_ibge", "crime_type", "total"])
    df["cod_ibge"] = df["cod_ibge"].astype("Int64")
    con.register("_temp_df", df)
    con.execute(f"""
        INSERT OR REPLACE INTO {TABLE_NAME}
        SELECT year, region, city, cod_ibge, crime_type, total
        FROM _temp_df
    """)
    con.unregister("_temp_df")


# ---------------------------------------------------------------------------
# WORKER -- scrapes one full year (runs in its own thread + Chrome instance)
# ---------------------------------------------------------------------------
def scrape_year(year: int, ibge_lookup: dict, con: duckdb.DuckDBPyConnection) -> int:
    """
    Opens a dedicated Chrome instance, scrapes all regions/cities for the
    given year, writes rows to DuckDB under the shared lock, then closes
    the browser.  Returns the total number of rows saved.
    """
    driver    = create_driver()
    total_rows = 0

    try:
        select_criminal_radio(driver)
        select_year(driver, year)

        regions = get_regions(driver)
        print(f"[year={year}] {len(regions)} regions found.")

        for region in regions:
            select_region(driver, region["value"], region["name"])
            cities = get_cities(driver)

            for city in cities:
                select_city(driver, city["value"], city["name"])

                cod_ibge = get_cod_ibge(city["name"], ibge_lookup)
                if cod_ibge is None:
                    with _unmatched_lock:
                        _unmatched_cities.add(city["name"])
                    print(f"  [year={year}] No IBGE match for '{city['name']}'")

                rows = scrape_table(driver, year, region["name"], city["name"], cod_ibge)

                if rows:
                    with _db_lock:
                        insert_rows(con, rows)
                    total_rows += len(rows)
                    print(f"  [year={year}] {city['name']} -> {len(rows)} rows saved.")

    finally:
        driver.quit()

    print(f"[year={year}] Done. {total_rows} rows total.")
    return total_rows


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    ibge_lookup = load_ibge_lookup(IBGE_CSV_PATH)
    con         = setup_database(DB_PATH)

    print(f"\n[main] Scraping {len(YEARS)} years with up to {MAX_WORKERS} parallel browsers...\n")

    grand_total = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_year, year, ibge_lookup, con): year
            for year in YEARS
        }
        for future in as_completed(futures):
            year = futures[future]
            try:
                grand_total += future.result()
            except Exception as exc:
                print(f"[year={year}] ERROR: {exc}")

    # Summary
    print(f"\n[main] All years done. Grand total: {grand_total} rows.")

    summary = con.execute(f"""
        SELECT year,
               COUNT(DISTINCT city)     AS cities,
               COUNT(DISTINCT cod_ibge) AS cities_with_ibge,
               COUNT(*)                 AS records
        FROM   {TABLE_NAME}
        GROUP  BY year
        ORDER  BY year
    """).fetchdf()
    print("\nRecords per year:")
    print(summary.to_string(index=False))

    if _unmatched_cities:
        print(f"\nCities with no IBGE match ({len(_unmatched_cities)}) -- stored with cod_ibge=NULL:")
        for c in sorted(_unmatched_cities):
            print(f"  - {c}")
    else:
        print("\nAll cities matched to an IBGE code.")

    con.close()
    print("\n[main] Database connection closed.")


if __name__ == "__main__":
    main()
