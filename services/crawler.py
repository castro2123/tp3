from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
import os
import re
import time
import unicodedata
import pandas as pd
import requests

URL = "https://live.euronext.com/pt/products/equities/list"
CSV_PATH = "data/Crawler/euronext_acoes.csv"
BUCKET_NAME = "data"
BUCKET_FILE_PATH = "Crawler/euronext_acoes.csv"

def load_env(filename=".env"):
    env = {}

    # diretório atual e diretório pai
    current_dir = os.path.abspath(os.getcwd())
    parent_dir = os.path.dirname(current_dir)

    possible_paths = [
        os.path.join(current_dir, filename),
        os.path.join(parent_dir, filename),
    ]

    env_path = None
    for path in possible_paths:
        if os.path.exists(path):
            env_path = path
            break

    if not env_path:
        print("Aviso: ficheiro .env não encontrado no diretório atual nem no diretório pai.")
        return env

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):]
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("'\"")

    return env


def validate_supabase_config(url, legacy_key, service_key, legacy_key_source):
    errors = []
    warnings = []
    if not url:
        errors.append("URL ausente no .env (variavel URL).")
    if not legacy_key:
        errors.append("LEGACY_KEY ausente no .env (ou KEY).")
    if url and not url.startswith("https://"):
        warnings.append("URL nao comeca com https://.")
    if url and "supabase.co" not in url:
        warnings.append("URL nao parece ser do Supabase.")
    if legacy_key_source == "KEY":
        warnings.append("LEGACY_KEY nao definido; usando KEY como legacy.")
    if service_key:
        warnings.append("SERVICE_KEY definido, mas o modo legacy usa apenas LEGACY_KEY/KEY.")
    return errors, warnings

def get_supabase_config():
    env = load_env()
    url = env.get("URL") or os.getenv("URL")
    service_key = env.get("SERVICE_KEY") or os.getenv("SERVICE_KEY")
    legacy_key = env.get("LEGACY_KEY") or os.getenv("LEGACY_KEY")
    legacy_key_source = "LEGACY_KEY"
    if not legacy_key:
        legacy_key = env.get("KEY") or os.getenv("KEY")
        legacy_key_source = "KEY"
    return url, legacy_key, service_key, legacy_key_source

def upload_to_bucket(file_path, bucket_name, file_name=None):
    url, legacy_key, service_key, legacy_key_source = get_supabase_config()
    errors, warnings = validate_supabase_config(url, legacy_key, service_key, legacy_key_source)
    for warning in warnings:
        print(f"Aviso: {warning}")
    if errors:
        print("Configuracao Supabase invalida:")
        for err in errors:
            print(f"- {err}")
        return False
    if not os.path.exists(file_path):
        print(f"Arquivo nao encontrado: {file_path}")
        return False

    key = legacy_key
    if not key:
        print("LEGACY_KEY/KEY nao definido. Upload cancelado.")
        return False

    if file_name is None:
        file_name = os.path.basename(file_path)

    storage_url = f"{url}/storage/v1/object/{bucket_name}/{file_name}"
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "x-upsert": "true",
        "Content-Type": "text/csv",
    }
    with open(file_path, "rb") as f:
        response = requests.post(storage_url, headers=headers, data=f)
    if response.status_code in (400, 401, 403):
        print(
            "Falha ao enviar para o bucket. Verifique se o bucket 'data' permite upload com a key atual "
            "ou defina SERVICE_KEY no .env para usar a service role."
        )
        return False
    if not response.ok:
        print(f"Falha ao enviar para o bucket: {response.status_code} {response.text}")
        return False
    response.raise_for_status()
    return True

def run_crawler():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    def normalize_header(text):
        text = text.strip().lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return " ".join(text.split())

    isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
    time_re = re.compile(r"\b\d{1,2}:\d{2}\b")
    month_re = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", re.I)
    price_re = re.compile(r"^\d+(?:[.,]\d+)?$")
    market_re = re.compile(r"^[A-Z0-9]{4}$")
    symbol_re = re.compile(r"^[A-Z0-9]{1,8}(?:\.[A-Z0-9]{1,3})?$")

    def clean_text(text):
        return text.replace("\xa0", " ").strip()

    def looks_like_market(text):
        return bool(text and market_re.match(text))

    def is_price(text):
        if not text:
            return False
        if "%" in text:
            return False
        if time_re.search(text) or month_re.search(text):
            return False
        if any(ch.isalpha() for ch in text):
            return False
        normalized = text.replace(" ", "").replace("\xa0", "")
        if normalized in {"-", "--"}:
            return False
        return bool(price_re.match(normalized))

    def map_headers(wait):
        try:
            headers = wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "table thead th")
                )
            )
        except TimeoutException:
            return {}
        mapped = {}
        for idx, header in enumerate(headers):
            label = normalize_header(header.text)
            if not label:
                continue
            if "nome" in label or "name" in label:
                mapped["name"] = idx
            elif "simbolo" in label or "symbol" in label:
                mapped["symbol"] = idx
            elif "mercado" in label or "market" in label:
                mapped["market"] = idx
            elif "ultimo" in label or "last" in label:
                mapped["last"] = idx
            elif "%" in label or "variacao" in label or "change" in label:
                mapped["change_pct"] = idx
            elif "data" in label or "hora" in label or "time" in label or "date" in label:
                mapped["datetime"] = idx
        return mapped

    def extrair_linhas(wait, header_map, dados):
        try:
            linhas = wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "table tbody tr")
                )
            )
        except TimeoutException:
            return

        for linha in linhas:
            try:
                colunas = linha.find_elements(By.TAG_NAME, "td")

                if not colunas:
                    continue

                def get_cell(idx):
                    if idx is None or idx >= len(colunas):
                        return ""
                    return colunas[idx].text.strip()

                def get_link_info():
                    try:
                        link_el = linha.find_element(By.TAG_NAME, "a")
                        return link_el.get_attribute("href") or "", link_el.text.strip()
                    except Exception:
                        return "", ""

                link, nome_link = get_link_info()
                market_code = ""
                if link and "-" in link:
                    market_code = link.rsplit("-", 1)[-1]

                data_by_label = {}
                for idx, col in enumerate(colunas):
                    label = col.get_attribute("data-label") or col.get_attribute("aria-label") or ""
                    label = normalize_header(label)
                    if label:
                        data_by_label[label] = clean_text(col.text)

                def get_by_label(tokens):
                    for label, value in data_by_label.items():
                        if any(token in label for token in tokens):
                            return value
                    return ""

                isin_val = get_by_label(["isin"])
                nome = get_by_label(["nome", "name"]) or nome_link or get_cell(header_map.get("name"))
                simbolo = get_by_label(["simbolo", "symbol", "ticker"]) or get_cell(header_map.get("symbol"))
                if not simbolo:
                    simbolo = isin_val
                mercado = get_by_label(["mercado", "market"]) or get_cell(header_map.get("market"))
                ultimo_preco = get_by_label(["ultimo", "last", "price", "preco"]) or get_cell(header_map.get("last"))
                variacao_percentual = get_by_label(["variacao", "change", "%"]) or get_cell(header_map.get("change_pct"))
                data_hora = get_by_label(["data", "hora", "time", "date"]) or get_cell(header_map.get("datetime"))

                texts = [clean_text(col.text) for col in colunas]
                if ultimo_preco == mercado or looks_like_market(ultimo_preco):
                    ultimo_preco = ""
                if not mercado and market_code:
                    mercado = market_code

                if not ultimo_preco:
                    isin_idx = next((i for i, t in enumerate(texts) if isin_re.match(t)), None)
                    change_idx = next((i for i, t in enumerate(texts) if "%" in t), None)
                    datetime_idx = next((i for i, t in enumerate(texts) if time_re.search(t) or month_re.search(t)), None)

                    market_idx = None
                    if market_code and market_code in texts:
                        market_idx = texts.index(market_code)
                    else:
                        market_idx = next((i for i, t in enumerate(texts) if looks_like_market(t)), None)

                    excluded = {idx for idx in [isin_idx, market_idx, change_idx, datetime_idx] if idx is not None}
                    last_idx = next((i for i, t in enumerate(texts) if is_price(t) and i not in excluded), None)

                    symbol_idx = next(
                        (i for i, t in enumerate(texts)
                         if symbol_re.match(t) and not looks_like_market(t)
                         and i not in excluded and i != last_idx),
                        None,
                    )

                    if symbol_idx is not None and not simbolo:
                        simbolo = texts[symbol_idx]
                    if market_idx is not None and (not mercado or not looks_like_market(mercado)):
                        mercado = texts[market_idx]
                    if last_idx is not None:
                        ultimo_preco = texts[last_idx]
                    else:
                        ultimo_preco = "-"
                    if change_idx is not None and not variacao_percentual:
                        variacao_percentual = texts[change_idx]
                    if datetime_idx is not None and not data_hora:
                        data_hora = texts[datetime_idx]

                dados.append({
                    "Name": nome,
                    "Símbolo": simbolo,
                    "Mercado": mercado,
                    "Último (Preço)": ultimo_preco,
                    "%": variacao_percentual,
                    "Data/Hora": data_hora,
                    "Link": link
                })

            except StaleElementReferenceException:
                continue

    dados = []
    try:
        driver.get(URL)
        wait = WebDriverWait(driver, 12)
        wait_short = WebDriverWait(driver, 5)
        header_map = map_headers(wait)
        if not header_map:
            header_map = {
                "name": 1,
                "symbol": 2,
                "market": 3,
                "last": 4,
                "change_pct": 6,
                "datetime": 7,
            }

        def get_active_page():
            selectors = [
                "a[aria-current='page']",
                "li.is-active a",
                "li.active a",
            ]
            for sel in selectors:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    text = el.text.strip()
                    if text:
                        return text
                except Exception:
                    continue
            return ""

        def click_next_page():
            selectors = [
                "a[rel='next']",
                "button[rel='next']",
                "a[aria-label*='Next']",
                "button[aria-label*='Next']",
                "a[aria-label*='Seguinte']",
                "button[aria-label*='Seguinte']",
                "li.pagination-next a",
                "li.next a",
                "a.next",
                "button.next",
            ]
            for sel in selectors:
                try:
                    for el in driver.find_elements(By.CSS_SELECTOR, sel):
                        if el.is_displayed() and el.is_enabled():
                            driver.execute_script("arguments[0].click();", el)
                            return True
                except Exception:
                    continue
            try:
                for el in driver.find_elements(
                    By.XPATH,
                    "//*[@aria-current='page']/parent::li/following-sibling::li[1]//a",
                ):
                    if el.is_displayed() and el.is_enabled():
                        driver.execute_script("arguments[0].click();", el)
                        return True
            except Exception:
                pass
            return False

        pagina = 1
        max_pages = 200
        while pagina <= max_pages:
            print(f"Pagina {pagina}")
            extrair_linhas(wait, header_map, dados)

            first_row = None
            try:
                first_row = wait_short.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
                )
            except TimeoutException:
                pass

            prev_page = get_active_page()
            if not click_next_page():
                break

            if prev_page:
                try:
                    wait_short.until(lambda d: get_active_page() != prev_page)
                except TimeoutException:
                    pass

            if first_row is not None:
                try:
                    wait_short.until(EC.staleness_of(first_row))
                except TimeoutException:
                    time.sleep(1)

            if prev_page and get_active_page() == prev_page:
                break

            pagina += 1
    finally:
        driver.quit()

    df = pd.DataFrame(dados)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

def main_loop():
    while True:
        start = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Iniciando crawler: {start}")
        try:
            run_crawler()
            uploaded = upload_to_bucket(CSV_PATH, BUCKET_NAME, BUCKET_FILE_PATH)
            if uploaded:
                print("CSV atualizado e enviado.")
            else:
                print("CSV atualizado, mas o upload falhou.")
        except Exception as exc:
            print(f"Falha: {exc}")
        time.sleep(300)

if __name__ == "__main__":
    main_loop()
