# TP3 - Integracao de Sistemas

## Serviços
- Crawler (Python): gera CSV e envia para Supabase bucket.
- Processador (Python): lê CSV em stream, enriquece, chama XML Service.
- XML Service (Python/FastAPI): gera XML de domínio, valida por XSD e grava em BD.
- BI Service (Node/Express): REST + GraphQL + visualização web.
- RPC Service (Python/XML-RPC): fornece metadados (mapper version).
- gRPC Service (Go): fornece hints de processamento (chunk/batch).

## Estrutura (pastas)
- `services/crawler` (crawler)
- `services/processor` (processador)
- `services/xml-service` (XML service)
- `services/bi-service` (BI service)
- `services/rpc-service` (XML-RPC)
- `services/grpc-service` (gRPC)

## Protocolos usados
- REST (Processor -> XML Service, BI -> XML Service).
- Webhook REST/JSON (XML Service -> Processor).
- GraphQL (BI Service).
- XML-RPC (Processor -> RPC Service).
- gRPC (Processor -> gRPC Service).

## BD
- Script: `services/xml-service/app/schema.sql`
- Colunas obrigatórias: `id`, `xml_document` (XML), `data_criacao`, `mapper_version`.

## BI Service
- UI: `http://localhost:3000`
- REST: `/api/markets`, `/api/sectors`, `/api/pe-by-sector`
- GraphQL: `/graphql`

## Docker Compose (ambiente docker)
- Subir: `docker compose up --build`
- XML Service: `http://localhost:8000`
- BI Service: `http://localhost:3000`
- RPC Service: `http://localhost:7000`
- gRPC Service: `localhost:6000`

## Variaveis principais
- `XML_SERVICE_BASE_URL` (ex: `http://localhost:8000`)
- `RPC_SERVICE_URL` (ex: `http://localhost:7000`)
- `GRPC_SERVICE_ADDR` (ex: `localhost:6000`)

## Env
- `.env` na raiz para execucao local.
- `env/` guarda os ficheiros antigos (`tp3.env`, `tp3-1.env`) para referencia.
