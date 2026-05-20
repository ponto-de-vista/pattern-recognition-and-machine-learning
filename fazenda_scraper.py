import os
import time
import glob
import logging
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
URL = "https://www.fazenda.sp.gov.br/SigeoLei131/Paginas/FlexConsDespesa.aspx"
DOWNLOAD_DIR = Path(os.getcwd()) / "downloads"
WAIT_TIMEOUT = 20

# Define the range of years you want to extract
YEARS_TO_SCRAPE = [str(year) for year in range(2010, 2020)]  # 2010 through 2019

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

# Base filters (Notice "Exercício" is removed here; it will be injected in the loop)
filtros_pesquisa = {
    "Fase": "Pago",
    "Órgão": "TODOS (Detalhado)",
    "Categoria": "TODAS (Consolidado)",
    "UO": "TODAS (Consolidado)",
    "Grupo": "TODOS (Consolidado)",
    "Unidade Gestora": "TODAS (Consolidado)",
    "Modalidade": "TODAS (Consolidado)",
    "Fonte de Recursos": "TODAS (Consolidado)",
    "Elemento": "TODOS (Consolidado)",
    "Licitação": "TODAS (Consolidado)",
    "Item": "TODOS (Detalhado)",
    "Função": "TODAS (Consolidado)",
    "Sub Função": "TODAS (Detalhado)",
    "Programa": "TODOS (Detalhado)",
    "Ação": "TODAS (Detalhado)",
    "Funcional Programática": "TODAS (Consolidado)",
    "Município": "TODOS (Detalhado)"
}

MAPA_IDS_DROPDOWNS = {
    "Exercício": "ctl00_ContentPlaceHolder1_ddlAno",
    "Órgão": "ctl00_ContentPlaceHolder1_ddlOrgao",
    "Categoria": "ctl00_ContentPlaceHolder1_ddlCategoria",
    "UO": "ctl00_ContentPlaceHolder1_ddlUo",
    "Grupo": "ctl00_ContentPlaceHolder1_ddlGrupo",
    "Unidade Gestora": "ctl00_ContentPlaceHolder1_ddlUge",
    "Modalidade": "ctl00_ContentPlaceHolder1_ddlModalidade",
    "Fonte de Recursos": "ctl00_ContentPlaceHolder1_ddlFonteRecursos",
    "Elemento": "ctl00_ContentPlaceHolder1_ddlElemento",
    "Licitação": "ctl00_ContentPlaceHolder1_ddlLicitacao",
    "Item": "ctl00_ContentPlaceHolder1_ddlItem",
    "Função": "ctl00_ContentPlaceHolder1_ddlFuncao",
    "Sub Função": "ctl00_ContentPlaceHolder1_ddlSubFuncao",
    "Programa": "ctl00_ContentPlaceHolder1_ddlPrograma",
    "Ação": "ctl00_ContentPlaceHolder1_ddlAcao",
    "Funcional Programática": "ctl00_ContentPlaceHolder1_ddlFuncionalProgramatica",
    "Município": "ctl00_ContentPlaceHolder1_ddlMunicipio"
}

# ---------------------------------------------------------------------------
# SELENIUM HELPERS
# ---------------------------------------------------------------------------
def setup_driver() -> webdriver.Chrome:
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    
    opts = Options()
    # opts.add_argument("--headless") # Uncomment to run in the background
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    opts.add_experimental_option("prefs", prefs)

    svc = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=svc, options=opts)
    return driver

def select_dropdown(driver: webdriver.Chrome, element_id: str, text_to_select: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    try:
        dropdown = wait.until(EC.presence_of_element_located((By.ID, element_id)))
        select = Select(dropdown)
        
        if select.first_selected_option.text.strip() == text_to_select:
            return 

        select.select_by_visible_text(text_to_select)
        log.info(f"Filter updated: {element_id} -> {text_to_select}")
        
        wait.until(EC.staleness_of(dropdown))
        
    except Exception as e:
        log.warning(f"Could not apply filter on ID {element_id} with text '{text_to_select}'. Error: {e}")

def set_fase_checkboxes(driver: webdriver.Chrome, fase_desejada: str) -> None:
    fases_ids = {
        "Dotação Inicial": "ctl00_ContentPlaceHolder1_cblFase_0",
        "Dotação Atual": "ctl00_ContentPlaceHolder1_cblFase_1",
        "Empenhado": "ctl00_ContentPlaceHolder1_cblFase_2",
        "Liquidado": "ctl00_ContentPlaceHolder1_cblFase_3",
        "Pago": "ctl00_ContentPlaceHolder1_cblFase_4"
    }
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    
    for nome, checkbox_id in fases_ids.items():
        try:
            checkbox = driver.find_element(By.ID, checkbox_id)
            is_checked = checkbox.is_selected()
            
            if nome != fase_desejada and is_checked:
                checkbox.click()
                wait.until(EC.staleness_of(checkbox))
            
            elif nome == fase_desejada and not is_checked:
                checkbox.click()
                wait.until(EC.staleness_of(checkbox))
                
        except Exception:
            log.warning(f"Error interacting with the checkbox for Phase '{nome}'")

def wait_for_download(download_dir: Path, timeout: int = 60) -> str | None:
    start_time = time.time()
    log.info("Waiting for the CSV file to download...")
    
    while time.time() - start_time < timeout:
        files = glob.glob(os.path.join(download_dir, "*.csv"))
        temp_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
        
        if files and not temp_files:
            latest_file = max(files, key=os.path.getctime)
            return latest_file
            
        time.sleep(1)
        
    return None

# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Starting scraper - SSP Fazenda")
    driver = setup_driver()
    
    try:
        # Loop through each year defined in the configuration
        for year in YEARS_TO_SCRAPE:
            log.info(f"==================================================")
            log.info(f"Starting extraction for year: {year}")
            log.info(f"==================================================")
            
            try:
                # Reload the URL for each year to ensure a clean slate
                driver.get(URL)
                wait = WebDriverWait(driver, WAIT_TIMEOUT)
                
                # Dynamically set the current year in the filter dictionary
                filtros_pesquisa["Exercício"] = year
                
                # 1. Apply Phase Checkbox
                set_fase_checkboxes(driver, filtros_pesquisa["Fase"])
                
                # 2. Apply Dropdowns
                for nome_filtro, element_id in MAPA_IDS_DROPDOWNS.items():
                    if nome_filtro in filtros_pesquisa:
                        select_dropdown(driver, element_id, filtros_pesquisa[nome_filtro])

                # 3. Click Search
                log.info("Clicking on 'Pesquisar' (Search)...")
                btn_pesquisar = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnPesquisar")))
                btn_pesquisar.click()
                
                log.info("Waiting for the results table to load...")
                
                # 4. Click Export
                btn_exportar = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Exportar em planilha ']")))
                btn_exportar.click()
                
                # 5. Wait for Download and Rename
                downloaded_file = wait_for_download(DOWNLOAD_DIR)
                
                if downloaded_file:
                    novo_nome = DOWNLOAD_DIR / f"fazenda_{year}.csv"
                    
                    if novo_nome.exists():
                        novo_nome.unlink()
                        
                    os.rename(downloaded_file, novo_nome)
                    log.info(f"SUCCESS! File saved as: {novo_nome.resolve()}")
                else:
                    log.error(f"Download timed out or failed for the year {year}.")
            
            except Exception as e:
                # If a specific year fails, log the error and continue to the next year
                log.error(f"An error occurred while processing the year {year}: {e}")
                continue 
            
    finally:
        driver.quit()
        log.info("Browser closed. Extraction complete.")

if __name__ == "__main__":
    main()