const statusText = document.getElementById("status-text");
const marketTotal = document.getElementById("market-total");
const sectorTotal = document.getElementById("sector-total");
const marketsBars = document.getElementById("markets-bars");
const sectorsBars = document.getElementById("sectors-bars");
const peBars = document.getElementById("pe-bars");
const recordsBody = document.getElementById("records-body");
const recordsTotal = document.getElementById("records-total");
const demoPill = document.getElementById("demo-pill");

const MAX_ROWS = 6;
const RECORD_LIMIT = 30;

window.addEventListener("load", () => {
  document.body.classList.add("loaded");
  loadData();
});

async function loadData() {
  try {
    const [health, markets, sectors, pe, records] = await Promise.all([
      fetchJson("/api/health"),
      fetchJson("/api/markets"),
      fetchJson("/api/sectors"),
      fetchJson("/api/pe-by-sector"),
      fetchJson(`/api/records?limit=${RECORD_LIMIT}`),
    ]);

    updateStatus(health);
    renderCounts(markets, marketsBars, marketTotal, "market");
    renderCounts(sectors, sectorsBars, sectorTotal, "sector");
    renderAverage(pe, peBars, "sector", "peRatio");
    renderRecords(records);
  } catch (err) {
    statusText.textContent = "XML service offline or unreachable.";
  }
}

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function updateStatus(health) {
  if (!health || health.status !== "ok") {
    statusText.textContent = "XML service unavailable.";
    return;
  }
  statusText.textContent = `XML service ready at ${health.xmlBaseUrl}`;
}

function renderCounts(payload, container, totalEl, key) {
  if (!payload || !Array.isArray(payload.data) || payload.data.length === 0) {
    totalEl.textContent = "-- items";
    container.innerHTML = '<p class="empty">No data yet.</p>';
    return;
  }

  totalEl.textContent = `${payload.total} items`;
  const rows = payload.data.slice(0, MAX_ROWS);
  renderBars(container, rows, key, "count", (value) => value);
}

function renderAverage(payload, container, labelKey, valueKey) {
  if (!payload || !Array.isArray(payload.data) || payload.data.length === 0) {
    container.innerHTML = '<p class="empty">No data yet.</p>';
    return;
  }

  const rows = payload.data.slice(0, MAX_ROWS);
  renderBars(
    container,
    rows,
    labelKey,
    valueKey,
    (value) => value.toFixed(2)
  );
}

function renderBars(container, rows, labelKey, valueKey, formatValue) {
  container.innerHTML = "";
  const maxValue = Math.max(
    ...rows.map((row) => (Number.isFinite(row[valueKey]) ? row[valueKey] : 0)),
    0
  );

  rows.forEach((row) => {
    const label = row[labelKey] || "Unknown";
    const value = Number.isFinite(row[valueKey]) ? row[valueKey] : 0;
    const ratio = maxValue ? value / maxValue : 0;

    const wrapper = document.createElement("div");
    wrapper.className = "bar-row";
    wrapper.style.setProperty("--bar", ratio.toFixed(4));

    const text = document.createElement("div");
    text.className = "bar-text";
    const labelEl = document.createElement("span");
    labelEl.className = "bar-label";
    labelEl.textContent = label;
    const subEl = document.createElement("span");
    subEl.className = "bar-sub";
    subEl.textContent = `${formatValue(value)}`;
    text.appendChild(labelEl);
    text.appendChild(subEl);

    const track = document.createElement("div");
    track.className = "bar-track";
    const fill = document.createElement("div");
    fill.className = "bar-fill";
    track.appendChild(fill);

    const valueEl = document.createElement("div");
    valueEl.className = "bar-value";
    valueEl.textContent = formatValue(value);

    wrapper.appendChild(text);
    wrapper.appendChild(track);
    wrapper.appendChild(valueEl);
    container.appendChild(wrapper);
  });
}

function renderRecords(payload) {
  if (!recordsBody) return;

  if (!payload || !Array.isArray(payload.data) || payload.data.length === 0) {
    if (recordsTotal) recordsTotal.textContent = "-- rows";
    recordsBody.innerHTML =
      '<tr class="empty-row"><td colspan="11">No data yet.</td></tr>';
    return;
  }

  if (recordsTotal) recordsTotal.textContent = `${payload.total} rows`;

  const columns = [
    { key: "nome", className: "cell-wide" },
    { key: "ticker" },
    { key: "mercado" },
    { key: "ultimoPreco" },
    { key: "variacaoPercentual" },
    { key: "dataHora", className: "cell-wide" },
    { key: "sector" },
    { key: "industria", className: "cell-wide" },
    { key: "marketCap" },
    { key: "peRatio" },
  ];

  recordsBody.innerHTML = "";

  payload.data.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      if (col.className) td.className = col.className;
      const value = row[col.key];

      td.textContent = value || "-";
      tr.appendChild(td);
    });
    recordsBody.appendChild(tr);
  });
}
