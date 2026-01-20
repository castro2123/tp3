from xmlrpc.client import ServerProxy
from config import RPC_SERVICE_URL, MAPPER_VERSION

def fetch_mapper_version():
    if not RPC_SERVICE_URL:
        return MAPPER_VERSION
    try:
        with ServerProxy(RPC_SERVICE_URL, allow_none=True) as proxy:
            return proxy.get_mapper_version()
    except Exception as exc:
        print(f"[RPC] Falha ao obter mapper version: {exc}")
        return MAPPER_VERSION
