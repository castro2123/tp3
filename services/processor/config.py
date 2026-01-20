import os

# Carrega .env manualmente
def load_env():
    env = {}
    current_dir = os.path.abspath(os.getcwd())
    parent_dir = os.path.dirname(current_dir)
    filenames = [".env", os.path.join("env", "tp3.env"), os.path.join("env", "tp3-1.env")]
    possible_paths = []
    for name in filenames:
        possible_paths.append(os.path.join(current_dir, name))
        possible_paths.append(os.path.join(parent_dir, name))

    found = False
    for env_path in possible_paths:
        if not os.path.exists(env_path):
            continue
        found = True
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[len("export "):]
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip().strip("'\"")

    if not found:
        print("Aviso: ficheiro .env não encontrado.")
    return env

# Aplica envs do ficheiro ao processo (sem sobrescrever vars existentes)
for key, value in load_env().items():
    os.environ.setdefault(key, value)

BUCKET_NAME = os.getenv("BUCKET_NAME", "data")
FILE_NAME = os.getenv("FILE_NAME", "Crawler/euronext_acoes.csv")
PROCESSED_PATH = os.getenv("PROCESSED_PATH", "data/Processed/acoes_enriched.csv")

WEBHOOK_XML_URL = os.getenv("WEBHOOK_XML_URL") or os.getenv("XML_SERVICE_URL")
JAVA_WEBHOOK_URL = os.getenv("JAVA_WEBHOOK_URL")
PROCESSOR_WEBHOOK_PORT = int(os.getenv("PROCESSOR_WEBHOOK_PORT", 5000))
MAPPER_VERSION = os.getenv("MAPPER_VERSION", "1.0")
RPC_SERVICE_URL = os.getenv("RPC_SERVICE_URL")
GRPC_SERVICE_ADDR = os.getenv("GRPC_SERVICE_ADDR")

PENDING_REQUESTS = {}

# Supabase config
def get_supabase_config():
    env = load_env()
    for key, value in env.items():
        os.environ.setdefault(key, value)
    url = os.getenv("URL")
    key = os.getenv("LEGACY_KEY") or os.getenv("KEY")
    return url, key
