import io
import os
import requests
import pandas as pd

BUCKET_NAME = "data"
FILE_NAME = "Crawler/euronext_acoes.csv"
LOCAL_PATH = "data/Supabase/SBdata.csv"

def load_env(path=".env"):
    if not os.path.exists(path):
        return {}
    env = {}
    with open(path, "r", encoding="utf-8") as f:
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

def download_from_bucket(bucket_name, file_name):
    url, legacy_key, service_key, legacy_key_source = get_supabase_config()
    errors, warnings = validate_supabase_config(url, legacy_key, service_key, legacy_key_source)
    for warning in warnings:
        print(f"Aviso: {warning}")
    if errors:
        print("Configuracao Supabase invalida:")
        for err in errors:
            print(f"- {err}")
        return None

    key = legacy_key
    if not url or not key:
        print("URL/LEGACY_KEY ausentes. Download cancelado.")
        return None

    storage_url = f"{url}/storage/v1/object/{bucket_name}/{file_name}"
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
    }
    response = requests.get(storage_url, headers=headers)
    if response.status_code in (400, 401, 403):
        storage_url = f"{url}/storage/v1/object/public/{bucket_name}/{file_name}"
        response = requests.get(storage_url, headers=headers)
    if response.status_code == 404:
        return None
    if not response.ok:
        print(f"Falha ao descarregar do bucket {bucket_name}: {response.status_code} {response.text}")
        return None
    return response.content

def main():
    content = download_from_bucket(BUCKET_NAME, FILE_NAME)
    if content is None:
        print(f"Nenhum arquivo encontrado no bucket {BUCKET_NAME}.")
        return
    local_dir = os.path.dirname(LOCAL_PATH)
    if local_dir:
        os.makedirs(local_dir, exist_ok=True)
    with open(LOCAL_PATH, "wb") as f:
        f.write(content)
    print(f"Bucket: {BUCKET_NAME} descarregado")

    df = pd.read_csv(io.BytesIO(content))
    print(df.head())

if __name__ == "__main__":
    main()
