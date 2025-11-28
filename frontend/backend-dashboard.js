const API_BASE =
  window.API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:8080`;
const API_KEY = window.API_KEY || "devkey";
const headers = API_KEY ? { "X-API-Key": API_KEY } : {};

function text(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

async function fetchJSON(url) {
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function loadStatus() {
  try {
    const health = await fetchJSON(`${API_BASE}/health`);
    text("last-updated", `Last updated: ${new Date().toLocaleTimeString()}`);
    text("mqtt-status", health.mqtt_broker || "--");
    text("db-status", health.db === "ok" ? "healthy" : "error");
    text("agg-status", "online");
    text("agg-detail", "Observations ➜ Aggregates ➜ Forecast");
    text("forecast-status", "OpenWeather");
    text("forecast-meta", "Configured provider");
  } catch (err) {
    console.error(err);
  }
}

async function loadFlow(cityId = "toronto") {
  try {
    const [obs, aggs, alerts] = await Promise.all([
      fetchJSON(`${API_BASE}/cities/${cityId}/observations?limit=50`),
      fetchJSON(`${API_BASE}/cities/${cityId}/aggregates?window=${encodeURIComponent("15 minutes")}&limit=10`),
      fetchJSON(`${API_BASE}/cities/${cityId}/alerts?limit=10`),
    ]);

    text("edge-count", `${obs.length} obs`);
    text("edge-detail", `${cityId} receiving data`);
    text("obs-count", obs.length.toString());
    text("agg-count", aggs.length.toString());
    text("stored-alerts", alerts.length.toString());
    text("alert-status", alerts.length ? "alerts stored" : "no alerts");
    text("alert-detail", alerts.length ? "Rules engine emitted alerts" : "Rules not triggered");
  } catch (err) {
    console.error(err);
  }
}

async function refreshAll() {
  await loadStatus();
  await loadFlow();
}

refreshAll();
setInterval(refreshAll, 12000);
