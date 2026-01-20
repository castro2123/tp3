import os

BUCKET_NAME = "data"
FILE_NAME = "Crawler/euronext_acoes.csv"
PROCESSED_PATH = "data/Processed/acoes_enriched.csv"

WEBHOOK_XML_URL = os.getenv("XML_SERVICE_URL")
JAVA_WEBHOOK_URL = os.getenv("JAVA_WEBHOOK_URL")
PROCESSOR_WEBHOOK_PORT = int(os.getenv("PROCESSOR_WEBHOOK_PORT", 5000))
MAPPER_VERSION = os.getenv('MAPPER_VERSION', '1.0')

PENDING_REQUESTS = {}

# Carrega .env manualmente
def load_env(filename=".env"):
    env = {}
    current_dir = os.path.abspath(os.getcwd())
    parent_dir = os.path.dirname(current_dir)
    possible_paths = [os.path.join(current_dir, filename), os.path.join(parent_dir, filename)]
    env_path = next((p for p in possible_paths if os.path.exists(p)), None)

    if not env_path:
        print("Aviso: ficheiro .env não encontrado.")
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

# Supabase config
def get_supabase_config():
    env = load_env()
    url = env.get("URL") or os.getenv("URL")
    key = env.get("LEGACY_KEY") or os.getenv("LEGACY_KEY") or os.getenv("KEY")
    return url, key
