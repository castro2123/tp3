from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import pandas as pd
import requests
import time
import os
import unicodedata
import re

URL = "https://live.euronext.com/pt/products/equities/list"
CSV_PATH = "data/Crawler/euronext_acoes.csv"
BUCKET_NAME = "data"
BUCKET_FILE_PATH = "Crawler/euronext_acoes.csv"

def load_env(path=".env"):
    if not os.path.exists(path):
        return {}
    env = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env

def upload_to_bucket(file_path, bucket_name, file_name=None):
    env = load_env()
    url = env.get("URL") or os.getenv("URL")
    key = env.get("KEY") or os.getenv("KEY")
    if not url or not key:
        raise ValueError("Missing URL/KEY for Supabase upload")

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
    response.raise_for_status()

def run_crawler():
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(URL)
    wait = WebDriverWait(driver, 15)

    dados = []

    def normalize_header(text):
        text = text.strip().lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return " ".join(text.split())

    def map_headers():
        headers = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "table thead th")
            )
        )
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

    header_map = map_headers()
    if not header_map:
        header_map = {
            "name": 1,
            "symbol": 2,
            "market": 3,
            "last": 4,
            "change_pct": 6,
            "datetime": 7,
        }

    def extrair_linhas():
        linhas = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "table tbody tr")
            )
        )

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
                    except:
                        return "", ""

                link, nome_link = get_link_info()

                data_by_label = {}
                for idx, col in enumerate(colunas):
                    label = col.get_attribute("data-label") or col.get_attribute("aria-label") or ""
                    label = normalize_header(label)
                    if label:
                        data_by_label[label] = col.text.strip()

                def get_by_label(tokens):
                    for label, value in data_by_label.items():
                        if any(token in label for token in tokens):
                            return value
                    return ""

                nome = get_by_label(["nome", "name"]) or nome_link or get_cell(header_map.get("name"))
                simbolo = get_by_label(["simbolo", "symbol"]) or get_cell(header_map.get("symbol"))
                mercado = get_by_label(["mercado", "market"]) or get_cell(header_map.get("market"))
                ultimo_preco = get_by_label(["ultimo", "last", "price", "preco"]) or get_cell(header_map.get("last"))
                variacao_percentual = get_by_label(["variacao", "change", "%"]) or get_cell(header_map.get("change_pct"))
                data_hora = get_by_label(["data", "hora", "time", "date"]) or get_cell(header_map.get("datetime"))

                texts = [col.text.strip() for col in colunas]
                if not ultimo_preco or (ultimo_preco.isupper() and len(ultimo_preco) == 4):
                    market_code = ""
                    if link and "-" in link:
                        market_code = link.rsplit("-", 1)[-1]

                    isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
                    time_re = re.compile(r"\b\d{1,2}:\d{2}\b")
                    month_re = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", re.I)
                    price_re = re.compile(r"^\d+(?:[.,]\d+)?$")

                    isin_idx = next((i for i, t in enumerate(texts) if isin_re.match(t)), None)
                    change_idx = next((i for i, t in enumerate(texts) if "%" in t), None)
                    datetime_idx = next((i for i, t in enumerate(texts) if time_re.search(t) or month_re.search(t)), None)

                    market_idx = None
                    if market_code and market_code in texts:
                        market_idx = texts.index(market_code)
                    else:
                        market_idx = next((i for i, t in enumerate(texts) if len(t) == 4 and t.isalnum() and t.isupper()), None)

                    last_idx = next((i for i, t in enumerate(texts) if price_re.match(t) and i not in {change_idx, datetime_idx}), None)

                    symbol_idx = next(
                        (i for i, t in enumerate(texts)
                         if t and len(t) <= 8 and t.replace(".", "").isalnum()
                         and i not in {isin_idx, market_idx, last_idx, change_idx, datetime_idx}),
                        None,
                    )

                    if symbol_idx is not None:
                        simbolo = texts[symbol_idx]
                    if market_idx is not None:
                        mercado = texts[market_idx]
                    if last_idx is not None:
                        ultimo_preco = texts[last_idx]
                    if change_idx is not None:
                        variacao_percentual = texts[change_idx]
                    if datetime_idx is not None:
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

    for pagina in range(1, 41):
        print(f"Pagina {pagina}")
        extrair_linhas()

        try:
            proxima = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//a[.='{pagina + 1}']")
                )
            )
            driver.execute_script("arguments[0].click();", proxima)
            time.sleep(2)
        except:
            break

    driver.quit()

    df = pd.DataFrame(dados)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

def main_loop():
    while True:
        start = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Iniciando crawler: {start}")
        try:
            run_crawler()
            upload_to_bucket(CSV_PATH, BUCKET_NAME, BUCKET_FILE_PATH)
            print("CSV atualizado e enviado.")
        except Exception as exc:
            print(f"Falha: {exc}")
        time.sleep(300)

if __name__ == "__main__":
    main_loop()
