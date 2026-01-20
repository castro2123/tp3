CREATE TABLE IF NOT EXISTS xml_documents (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    xml_document XML NOT NULL,
    data_criacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mapper_version VARCHAR(32) NOT NULL,
    status VARCHAR(24) NOT NULL
);
