from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import os
from datetime import datetime, timezone

RPC_HOST = os.getenv("RPC_HOST", "0.0.0.0")
RPC_PORT = int(os.getenv("RPC_PORT", "7000"))

def ping():
    return "pong"

def get_mapper_version():
    return os.getenv("MAPPER_VERSION", "1.0")

def get_domain_info():
    return {
        "root": "RelatorioMercado",
        "entity": "Ativo",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

def main():
    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ("/", "/RPC2")

    server = SimpleXMLRPCServer(
        (RPC_HOST, RPC_PORT),
        requestHandler=RequestHandler,
        allow_none=True
    )
    server.register_function(ping, "ping")
    server.register_function(get_mapper_version, "get_mapper_version")
    server.register_function(get_domain_info, "get_domain_info")
    print(f"[RPC] XML-RPC on {RPC_HOST}:{RPC_PORT}")
    server.serve_forever()

if __name__ == "__main__":
    main()
