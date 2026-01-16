import os
import io
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
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env

def download_from_bucket(bucket_name, file_name):
    env = load_env()
    url = env.get("URL") or os.getenv("URL")
    key = env.get("KEY") or os.getenv("KEY")
    if not url or not key:
        raise ValueError("Missing URL/KEY for Supabase download")

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
