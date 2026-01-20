const path = require("path");
const express = require("express");
const axios = require("axios");
const dotenv = require("dotenv");
const { graphqlHTTP } = require("express-graphql");
const { buildSchema } = require("graphql");

dotenv.config();

const app = express();
const port = Number(process.env.BI_PORT || 3000);
const xmlBaseUrl = resolveXmlBaseUrl();
const demoMode = isTruthy(process.env.DEMO_MODE);

app.use(express.static(path.join(__dirname, "public")));

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok", xmlBaseUrl, demoMode });
});

app.get("/api/markets", async (_req, res) => {
  try {
    const results = await fetchXPath("//*[local-name()='Ativo']/@mercado");
    const values = flattenMatches(results);
    const summary = countBy(values, "market");
    res.json(summary);
  } catch (err) {
    res.status(502).json({ error: "failed_to_query_xml_service" });
  }
});

app.get("/api/sectors", async (_req, res) => {
  try {
    const results = await fetchXPath(
      "//*[local-name()='Empresa']/*[local-name()='Sector']/text()"
    );
    const values = flattenMatches(results);
    const summary = countBy(values, "sector");
    res.json(summary);
  } catch (err) {
    res.status(502).json({ error: "failed_to_query_xml_service" });
  }
});

app.get("/api/pe-by-sector", async (_req, res) => {
  try {
    const sectors = await fetchXPath(
      "//*[local-name()='Empresa']/*[local-name()='Sector']/text()"
    );
    const peRatios = await fetchXPath(
      "//*[local-name()='Fundamentos']/*[local-name()='PERatio']/text()"
    );
    const pairs = pairByDocument(sectors, peRatios);
    const summary = averageByCategory(pairs, "sector", "peRatio");
    res.json(summary);
  } catch (err) {
    res.status(502).json({ error: "failed_to_query_xml_service" });
  }
});

app.get("/api/records", async (req, res) => {
  try {
    const limit = Number.parseInt(req.query.limit, 10) || 30;
    const results = await fetchXPath(
      `//*[local-name()='Ativo'][position()<=${limit}]`
    );
    const matches = selectLatestMatches(results);
    const rows = matches.map(parseAtivoXml);
    res.json({ total: rows.length, data: rows });
  } catch (err) {
    console.error("[BI] records error:", err?.message || err);
    res.status(502).json({
      error: "failed_to_query_xml_service",
      detail: err?.message || String(err),
    });
  }
});

const schema = buildSchema(`
  type CountItem {
    market: String
    sector: String
    count: Int!
    share: Float!
  }

  type CountSummary {
    total: Int!
    data: [CountItem!]!
  }

  type PEAverage {
    sector: String!
    peRatio: Float!
    count: Int!
  }

  type PEReport {
    data: [PEAverage!]!
  }

  type Query {
    markets: CountSummary!
    sectors: CountSummary!
    peBySector: PEReport!
  }
`);

const root = {
  markets: async () => {
    const results = await fetchXPath("//*[local-name()='Ativo']/@mercado");
    const values = flattenMatches(results);
    return countBy(values, "market");
  },
  sectors: async () => {
    const results = await fetchXPath(
      "//*[local-name()='Empresa']/*[local-name()='Sector']/text()"
    );
    const values = flattenMatches(results);
    return countBy(values, "sector");
  },
  peBySector: async () => {
    const sectors = await fetchXPath(
      "//*[local-name()='Empresa']/*[local-name()='Sector']/text()"
    );
    const peRatios = await fetchXPath(
      "//*[local-name()='Fundamentos']/*[local-name()='PERatio']/text()"
    );
    const pairs = pairByDocument(sectors, peRatios);
    return averageByCategory(pairs, "sector", "peRatio");
  },
};

app.use(
  "/graphql",
  graphqlHTTP({
    schema,
    rootValue: root,
    graphiql: true,
  })
);

app.listen(port, () => {
  console.log(`[BI] Listening on port ${port}`);
  console.log(`[BI] XML Service base URL: ${xmlBaseUrl}`);
});

function resolveXmlBaseUrl() {
  const direct = process.env.XML_SERVICE_BASE_URL;
  if (direct) return direct.replace(/\/+$/, "");

  const fallback =
    process.env.XML_SERVICE_URL ||
    process.env.WEBHOOK_XML_URL ||
    "http://localhost:8000";

  if (fallback.endsWith("/process_csv")) {
    return fallback.slice(0, -"/process_csv".length).replace(/\/+$/, "");
  }

  return fallback.replace(/\/+$/, "");
}

function isTruthy(value) {
  if (!value) return false;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

async function fetchXPath(xpath) {
  const url = `${xmlBaseUrl}/query_xml`;
  const response = await axios.get(url, {
    params: { xpath, latest: 1 },
    timeout: 10000,
  });
  return response.data && response.data.results ? response.data.results : [];
}

function flattenMatches(results) {
  return results.flatMap((row) => (Array.isArray(row.matches) ? row.matches : []));
}

function normalizeText(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text.length ? text : null;
}

function countBy(values, keyName) {
  const counts = new Map();
  let total = 0;

  for (const value of values) {
    const normalized = normalizeText(value);
    if (!normalized) continue;
    total += 1;
    counts.set(normalized, (counts.get(normalized) || 0) + 1);
  }

  const data = Array.from(counts.entries())
    .map(([name, count]) => ({
      [keyName]: name,
      count,
      share: total ? count / total : 0,
    }))
    .sort((a, b) => b.count - a.count);

  return { total, data };
}

function pairByDocument(resultsA, resultsB) {
  const pairs = [];
  const max = Math.max(resultsA.length, resultsB.length);

  for (let i = 0; i < max; i += 1) {
    const valuesA = Array.isArray(resultsA[i]?.matches) ? resultsA[i].matches : [];
    const valuesB = Array.isArray(resultsB[i]?.matches) ? resultsB[i].matches : [];
    const len = Math.min(valuesA.length, valuesB.length);

    for (let j = 0; j < len; j += 1) {
      pairs.push([valuesA[j], valuesB[j]]);
    }
  }

  return pairs;
}

function parseNumber(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).replace(/[,\s]/g, "");
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function averageByCategory(pairs, categoryKey, valueKey) {
  const buckets = new Map();

  for (const [category, value] of pairs) {
    const label = normalizeText(category);
    const numeric = parseNumber(value);
    if (!label || numeric === null) continue;

    const current = buckets.get(label) || { total: 0, count: 0 };
    current.total += numeric;
    current.count += 1;
    buckets.set(label, current);
  }

  const data = Array.from(buckets.entries())
    .map(([label, stats]) => ({
      [categoryKey]: label,
      [valueKey]: stats.count ? stats.total / stats.count : 0,
      count: stats.count,
    }))
    .sort((a, b) => b[valueKey] - a[valueKey]);

  return { data };
}

function selectLatestMatches(results) {
  if (!Array.isArray(results) || results.length === 0) return [];
  const latest = results.reduce((max, row) => (row.id > max.id ? row : max), results[0]);
  return Array.isArray(latest.matches) ? latest.matches : [];
}

function parseAtivoXml(xml) {
  if (!xml) return {};
  const ticker = extractAttr(xml, "ticker");
  const mercado = extractAttr(xml, "mercado");
  return {
    nome: extractTag(xml, "Nome"),
    ticker,
    mercado,
    ultimoPreco: extractTag(xml, "UltimoPreco"),
    variacaoPercentual: extractTag(xml, "VariacaoPercentual"),
    dataHora: extractTag(xml, "DataHora"),
    link: extractTag(xml, "Link"),
    sector: extractTag(xml, "Sector"),
    industria: extractTag(xml, "Industria"),
    marketCap: extractTag(xml, "MarketCap"),
    peRatio: extractTag(xml, "PERatio"),
  };
}

function extractAttr(xml, name) {
  const regex = new RegExp(`${name}=\"([^\"]*)\"`, "i");
  const match = xml.match(regex);
  return match ? normalizeText(decodeXml(match[1])) : null;
}

function extractTag(xml, tag) {
  const regex = new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = xml.match(regex);
  return match ? normalizeText(decodeXml(match[1])) : null;
}

function decodeXml(value) {
  return String(value)
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'");
}
