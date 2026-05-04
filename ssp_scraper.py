"""
SSP-SP Criminal Statistics Scraper
====================================
Source : https://www.ssp.sp.gov.br/estatistica/dados-mensais
Output : crime-<year>.parquet  (one file per year)

Output schema — one row per (city × month):
    year | region | city | cod_ibge | month
    | <CRIME_TYPE_1> | <CRIME_TYPE_2> | ...  (one column per crime type)

The "Total" column from the website is dropped — compute totals with
    df.filter(like='CRIME_').sum(axis=1)
or sum across months with
    df.groupby(['year','city','cod_ibge'])[crime_cols].sum()

City-name matching
------------------
SSP city names are normalised with the same pipeline applied to the IBGE
lookup so that accents, hyphens, apostrophes and known typos never cause a
failed join. The goal is exactly 645 distinct cod_ibge values with no NULLs.

Two correction buckets are used:
  _EXACT_CORRECTIONS  — full-name match only. Safe for names that are
                        substrings of other cities (e.g. "Embu" inside
                        "Embu das Artes" and "Embu-Guaçu").
  _SUBSTR_CORRECTIONS — str.replace (substring). Only used when the pattern
                        is guaranteed not to appear inside any other city name.

Years already saved as parquet are skipped automatically on restart.

Dependencies
------------
    pip install selenium webdriver-manager pandas pyarrow
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
URL            = "https://www.ssp.sp.gov.br/estatistica/dados-mensais"
YEARS          = list(range(2009, 2019))        # 2009–2018 inclusive
OUTPUT_DIR     = Path(".")                       # where parquet files land
IBGE_CSV_PATH  = Path("codigos_municipios_regioes.csv")
WAIT_TIMEOUT   = 15                              # seconds per Selenium wait

MONTH_NAMES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
               "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

# ---------------------------------------------------------------------------
# CITY NAME CORRECTIONS  —  SSP site spelling -> IBGE official spelling
# ---------------------------------------------------------------------------
# _EXACT_CORRECTIONS: checked against the full city name (strip + dict lookup).
# Use this for any name that is a substring of another SP city — avoids the
# str.replace bug where "Embu" -> "Embu das Artes" would also corrupt
# "Embu das Artes" -> "Embu das Artes das Artes" and
# "Embu-Guaçu"     -> "Embu das Artes-Guaçu".
_EXACT_CORRECTIONS: dict[str, str] = {
    "Embu":                  "Embu das Artes",   # renamed 2011; SSP kept old name
    "Santa Rosa do Viterbo": "Santa Rosa de Viterbo",  # SSP uses 'do', IBGE uses 'de'
    "Brodosqui":             "Brodowski",         # old spelling, renamed 1944
    "Moji das Cruzes":       "Mogi das Cruzes",   # archaic spelling
    "Florínia":              "Florínea",           # SSP typo
}

# _SUBSTR_CORRECTIONS: applied as str.replace. Only safe when the pattern
# cannot appear as a substring of any other SP city name.
_SUBSTR_CORRECTIONS: list[tuple[str, str]] = [
    ("São Luís do", "São Luiz do"),   # unique prefix in SP — safe as substring
]

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NORMALISATION HELPERS
# ---------------------------------------------------------------------------
def _apply_corrections(text: str) -> str:
    """
    Apply SSP->IBGE corrections before accent-stripping.
    Exact corrections are checked first (full-string match).
    Substring corrections are applied only if no exact match was found.
    """
    stripped = text.strip()
    if stripped in _EXACT_CORRECTIONS:
        return _EXACT_CORRECTIONS[stripped]
    for wrong, right in _SUBSTR_CORRECTIONS:
        text = text.replace(wrong, right)
    return text


def normalize(text: str) -> str:
    """
    Canonical key for matching city names across SSP and IBGE:
      1. Exact correction (full-name dict lookup)
      2. Substring corrections (safe prefixes only)
      3. Strip accents via NFKD
      4. Remove hyphens and apostrophes
      5. Uppercase + collapse whitespace
    """
    if not isinstance(text, str):
        return ""
    text  = _apply_corrections(text)
    nfkd  = unicodedata.normalize("NFKD", text)
    ascii_ = nfkd.encode("ascii", "ignore").decode()
    ascii_ = ascii_.replace("-", " ").replace("'", " ")
    return " ".join(ascii_.upper().split())


def crime_type_to_col(raw: str) -> str:
    """
    Convert a raw crime-type label into a clean, stable column name.
    Trailing footnote markers are stripped:
        'HOMICÍDIO DOLOSO (2)'        -> 'HOMICIDIO_DOLOSO'
        'TOTAL DE ROUBO - OUTROS (1)' -> 'TOTAL_DE_ROUBO_OUTROS'
        'FURTO - OUTROS'              -> 'FURTO_OUTROS'
    """
    cleaned = re.sub(r"\s*\(\d+\)\s*$", "", raw.strip())
    nfkd    = unicodedata.normalize("NFKD", cleaned)
    ascii_  = nfkd.encode("ascii", "ignore").decode()
    col     = re.sub(r"[^A-Z0-9]+", "_", ascii_.upper())
    return col.strip("_")


# ---------------------------------------------------------------------------
# IBGE LOOKUP
# ---------------------------------------------------------------------------
def build_ibge_lookup(csv_path: Path) -> dict[str, int]:
    """
    Returns {normalize(city_name): cod_ibge} for all 645 SP municipalities.
    Rows without a 7-digit cod_ibge and the placeholder row are skipped.
    """
    df = pd.read_csv(csv_path, encoding="latin1", sep=None, engine="python")
    df = df[["cod_ibge", "municipio"]].dropna()
    df = df[df["cod_ibge"].astype(str).str.len() == 7]
    df = df[~df["municipio"].str.startswith("Sem especifica")]

    lookup: dict[str, int] = {}
    for _, row in df.iterrows():
        key = normalize(row["municipio"])
        lookup[key] = int(row["cod_ibge"])

    log.info("[ibge] Lookup built: %d cities.", len(lookup))
    return lookup


# ---------------------------------------------------------------------------
# RAW SCRAPED DATA (before pivot)
# ---------------------------------------------------------------------------
@dataclass
class RawRow:
    """One crime-type × month observation for a single city."""
    year:       int
    region:     str
    city:       str
    cod_ibge:   int | None
    month:      int         # 1–12
    crime_col:  str         # sanitised column name
    value:      int | None


# ---------------------------------------------------------------------------
# BROWSER SETUP
# ---------------------------------------------------------------------------
def create_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    svc    = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=svc, options=opts)
    driver.get(URL)
    return driver


# ---------------------------------------------------------------------------
# PAGE INTERACTIONS
# ---------------------------------------------------------------------------
def select_criminal_radio(driver: webdriver.Chrome) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "input[type='radio']")))
    for label in driver.find_elements(By.CSS_SELECTOR, "label.radio-inline"):
        if "Criminal" in label.text and "Produtividade" not in label.text:
            driver.execute_script(
                "arguments[0].click();",
                label.find_element(By.TAG_NAME, "input"),
            )
            return
    raise RuntimeError("'Criminal' radio button not found.")


def select_year(driver: webdriver.Chrome, year: int) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el   = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalAno']")
    ))
    Select(el).select_by_value(str(year))
    time.sleep(0.5)


def get_regions(driver: webdriver.Chrome) -> list[dict]:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el   = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalRegiao']")
    ))
    return [
        {"value": opt.get_attribute("value"), "name": opt.text.strip()}
        for opt in Select(el).options
        if opt.get_attribute("value") not in ("0", "")
    ]


def select_region(driver: webdriver.Chrome, region_value: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el   = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalRegiao']")
    ))
    Select(el).select_by_value(region_value)
    time.sleep(1)


def get_cities(driver: webdriver.Chrome) -> list[dict]:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el   = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalMunicipio']")
    ))
    return [
        {"value": opt.get_attribute("value"), "name": opt.text.strip()}
        for opt in Select(el).options
        if opt.get_attribute("value") not in ("0", "")
    ]


def select_city(driver: webdriver.Chrome, city_value: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    el   = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "select[formcontrolname='mensalMunicipio']")
    ))
    Select(el).select_by_value(city_value)
    time.sleep(2)


# ---------------------------------------------------------------------------
# TABLE PARSING
# ---------------------------------------------------------------------------
def parse_int(raw: str) -> int | None:
    """'13.998' -> 13998 ;  empty / dash -> None."""
    cleaned = raw.strip().replace(".", "").replace(",", "").replace("-", "")
    return int(cleaned) if cleaned.isdigit() else None


def _snapshot_tables(driver: webdriver.Chrome) -> list[dict]:
    """
    Read all accordion tables in a single JS call — no Selenium element
    references survive, so StaleElementReferenceException cannot occur.
    """
    return driver.execute_script("""
        const tables = document.querySelectorAll('.accordion-body table');
        return Array.from(tables).map(table => {
            const headers = Array.from(
                table.querySelectorAll('thead th')
            ).map(th => th.innerText.trim());

            const rows = Array.from(
                table.querySelectorAll('tbody tr')
            ).map(tr => {
                const label = tr.querySelector('th')
                    ? tr.querySelector('th').innerText.trim()
                    : null;
                const cells = Array.from(
                    tr.querySelectorAll('td')
                ).map(td => td.innerText.trim());
                return {label, cells};
            }).filter(r => r.label !== null);

            return {headers, rows};
        });
    """)


def scrape_table(
    driver:      webdriver.Chrome,
    year:        int,
    region_name: str,
    city_name:   str,
    cod_ibge:    int | None,
    max_retries: int = 3,
) -> list[RawRow]:
    """
    Snapshot every accordion table into plain strings via a single JS call,
    then parse locally. Retries up to max_retries times for slow Angular renders.
    """
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    try:
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, ".accordion-body table")
        ))
    except TimeoutException:
        log.warning(
            "  No table for '%s / %s' (year=%d) — skipping.",
            region_name, city_name, year,
        )
        return []

    snapshots = []
    for attempt in range(1, max_retries + 1):
        snapshots = _snapshot_tables(driver)
        if snapshots:
            break
        log.warning(
            "  Empty snapshot for '%s' (attempt %d/%d) — retrying.",
            city_name, attempt, max_retries,
        )
        time.sleep(1)

    raw_rows: list[RawRow] = []

    for table in snapshots:
        headers  = table["headers"]

        # headers[0] is 'Natureza' (<th scope="row">), so td index = header index - 1
        month_td: dict[int, int] = {}
        for i, h in enumerate(headers):
            if h in MONTH_NAMES:
                month_td[MONTH_NAMES.index(h) + 1] = i - 1  # Jan=1 … Dez=12

        if not month_td:
            continue

        for row in table["rows"]:
            crime_col = crime_type_to_col(row["label"])
            cells     = row["cells"]

            for month_num, td_idx in month_td.items():
                value = parse_int(cells[td_idx]) if 0 <= td_idx < len(cells) else None

                raw_rows.append(RawRow(
                    year=year,
                    region=region_name,
                    city=city_name,
                    cod_ibge=cod_ibge,
                    month=month_num,
                    crime_col=crime_col,
                    value=value,
                ))

    return raw_rows


# ---------------------------------------------------------------------------
# YEAR PIPELINE  —  all regions -> all cities -> scrape -> pivot
# ---------------------------------------------------------------------------
def scrape_year(year: int, ibge_lookup: dict[str, int]) -> pd.DataFrame:
    """
    Collect every region / city for *year* and return a pivoted DataFrame:
        year | region | city | cod_ibge | month | <CRIME_COL_1> | ...
    """
    log.info("[year=%d] Starting browser.", year)
    driver    = create_driver()
    raw_rows: list[RawRow] = []
    unmatched: set[str]    = set()

    try:
        select_criminal_radio(driver)
        select_year(driver, year)

        regions = get_regions(driver)
        log.info("[year=%d] %d region(s) found.", year, len(regions))

        for r_idx, region in enumerate(regions, 1):
            log.info("[year=%d] [%d/%d] Region: %s",
                     year, r_idx, len(regions), region["name"])
            select_region(driver, region["value"])
            cities = get_cities(driver)

            for c_idx, city in enumerate(cities, 1):
                select_city(driver, city["value"])

                # normalize() applies _EXACT_CORRECTIONS first (full-name match)
                # then _SUBSTR_CORRECTIONS — no substring collision possible
                cod_ibge = ibge_lookup.get(normalize(city["name"]))
                if cod_ibge is None:
                    unmatched.add(city["name"])
                    log.warning(
                        "  [year=%d] No IBGE match for '%s'", year, city["name"]
                    )

                rows = scrape_table(
                    driver, year, region["name"], city["name"], cod_ibge
                )
                raw_rows.extend(rows)

                log.info(
                    "  [year=%d] [%d/%d] %-35s -> %d obs",
                    year, c_idx, len(cities), city["name"], len(rows),
                )

    finally:
        driver.quit()
        log.info("[year=%d] Browser closed. Raw obs: %d", year, len(raw_rows))

    if not raw_rows:
        return pd.DataFrame()

    if unmatched:
        log.warning(
            "[year=%d] %d city/cities with no IBGE match: %s",
            year, len(unmatched), sorted(unmatched),
        )

    return _pivot(raw_rows)


# ---------------------------------------------------------------------------
# PIVOT  —  long (crime_col, value) -> wide (one column per crime type)
# ---------------------------------------------------------------------------
def _pivot(raw_rows: list[RawRow]) -> pd.DataFrame:
    """
    Input:  year, region, city, cod_ibge, month, crime_col, value  (long)
    Output: year, region, city, cod_ibge, month, <COL_A>, <COL_B>… (wide)
    One row per (city × month).
    """
    df = pd.DataFrame([
        {
            "year":      r.year,
            "region":    r.region,
            "city":      r.city,
            "cod_ibge":  r.cod_ibge,
            "month":     r.month,
            "crime_col": r.crime_col,
            "value":     r.value,
        }
        for r in raw_rows
    ])

    pivoted = df.pivot_table(
        index=["year", "region", "city", "cod_ibge", "month"],
        columns="crime_col",
        values="value",
        aggfunc="first",
    ).reset_index()

    pivoted.columns.name = None
    pivoted = pivoted.sort_values(["region", "city", "month"]).reset_index(drop=True)

    pivoted["cod_ibge"] = pivoted["cod_ibge"].astype("Int64")

    crime_cols = [c for c in pivoted.columns
                  if c not in ("year", "region", "city", "cod_ibge", "month")]
    for col in crime_cols:
        pivoted[col] = pd.to_numeric(pivoted[col], errors="coerce").astype("Int64")

    return pivoted


# ---------------------------------------------------------------------------
# PARQUET OUTPUT
# ---------------------------------------------------------------------------
def save_parquet(df: pd.DataFrame, year: int, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"crime-{year}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")

    n_cities  = df["city"].nunique()
    n_ibge    = df["cod_ibge"].nunique()
    n_missing = df["cod_ibge"].isna().sum()
    log.info(
        "Saved -> %s  |  rows=%d  cities=%d  cod_ibge=%d  missing_ibge=%d",
        path, len(df), n_cities, n_ibge, n_missing,
    )
    if n_ibge != 645:
        log.warning(
            "  Expected 645 distinct cod_ibge but got %d — check unmatched cities above.",
            n_ibge,
        )
    return path


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Years      : %s", YEARS)
    log.info("Output dir : %s", OUTPUT_DIR.resolve())

    ibge_lookup = build_ibge_lookup(IBGE_CSV_PATH)

    saved_files: list[Path] = []

    for year in YEARS:
        # Skip years already saved — safe to restart after interruption
        parquet_path = OUTPUT_DIR / f"crime-{year}.parquet"
        if parquet_path.exists():
            log.info("[year=%d] Already exists — skipping. (%s)", year, parquet_path)
            saved_files.append(parquet_path)
            continue

        df = scrape_year(year, ibge_lookup)

        if df.empty:
            log.warning("[year=%d] No data — parquet NOT written.", year)
            continue

        path = save_parquet(df, year, OUTPUT_DIR)
        saved_files.append(path)

        summary = (
            df.groupby("region")["city"]
            .nunique()
            .reset_index()
            .rename(columns={"city": "cities"})
        )
        log.info("[year=%d] Cities per region:\n%s", year, summary.to_string(index=False))

    log.info("\nDone. %d parquet file(s) written:", len(saved_files))
    for p in saved_files:
        log.info("  %s", p)


if __name__ == "__main__":
    main()