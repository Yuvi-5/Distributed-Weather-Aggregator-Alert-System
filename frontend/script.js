const API_BASE = window.API_BASE || "http://localhost:8080";
const API_KEY = "devkey"; // match infra/docker-compose.yml; change for prod
const MQTT_URL = window.MQTT_URL || "ws://localhost:9001";

const CITY_CONFIG = {
    Brampton: { id: "brampton", lat: 43.7315, lon: -79.7624 },
    Mississauga: { id: "mississauga", lat: 43.589, lon: -79.6441 },
    Toronto: { id: "toronto", lat: 43.6532, lon: -79.3832 },
};

const citySelect = document.getElementById("city");
const cityName = document.getElementById("city-name");
const updatedTime = document.getElementById("updated-time");
const heroSubtitle = document.getElementById("hero-subtitle");
const tempNow = document.getElementById("temp-now");
const conditionEl = document.getElementById("condition");
const highLowEl = document.getElementById("high-low");
const hourlyCards = document.getElementById("hourly-cards");
const weeklyCards = document.getElementById("weekly-cards");
const alertsList = document.getElementById("alerts-list");

const humidityValue = document.getElementById("humidity-value");
const humiditySub = document.getElementById("humidity-sub");
const windValue = document.getElementById("wind-value");
const windSub = document.getElementById("wind-sub");
const pressureValue = document.getElementById("pressure-value");
const rainValue = document.getElementById("rain-value");
const rainSub = document.getElementById("rain-sub");

const headers = API_KEY ? { "X-API-Key": API_KEY } : {};

const gradientClasses = ["gradient-1", "gradient-2", "gradient-3"];
const softClasses = ["soft-1", "soft-2", "soft-3"];

const formatTemp = (v) =>
    typeof v === "number" ? `${v.toFixed(1)}°C` : "--";

const formatPct = (v) =>
    typeof v === "number" ? `${Math.round(v * 100)}%` : "--";

const formatWind = (v) =>
    typeof v === "number" ? `${v.toFixed(1)} kph` : "--";

const formatPressure = (v) =>
    typeof v === "number" ? `${v.toFixed(0)} hPa` : "--";

const formatRain = (v) =>
    typeof v === "number" ? `${v.toFixed(1)} mm` : "--";

const fmtTime = (iso) =>
    iso ? new Date(iso).toLocaleString([], { hour: "numeric", minute: "2-digit" }) : "--";

async function fetchJSON(url) {
    const res = await fetch(url, { headers });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }
    return res.json();
}

function renderCurrent(observations = []) {
    const latest = observations[0];
    if (!latest) {
        tempNow.textContent = "--";
        heroSubtitle.textContent = "No observations yet";
        conditionEl.textContent = "Waiting for edge nodes to publish.";
        highLowEl.textContent = "High: -- Low: --";
        updatedTime.textContent = "Last Updated: --";
        humidityValue.textContent = "--";
        windValue.textContent = "--";
        pressureValue.textContent = "--";
        rainValue.textContent = "--";
        return;
    }

    tempNow.textContent = formatTemp(latest.temp_c);
    heroSubtitle.textContent = `Latest from ${latest.source} @ ${fmtTime(latest.observed_at)}`;
    conditionEl.textContent = [
        `Humidity ${formatPct(latest.humidity)}`,
        `Wind ${formatWind(latest.wind_kph)}`,
        `Pressure ${formatPressure(latest.pressure_hpa)}`,
    ].join(" | ");

    const temps = observations
        .map((o) => o.temp_c)
        .filter((t) => typeof t === "number");
    const max = temps.length ? Math.max(...temps) : null;
    const min = temps.length ? Math.min(...temps) : null;
    highLowEl.textContent = `High: ${formatTemp(max)}  Low: ${formatTemp(min)}`;
    updatedTime.textContent = `Last Updated: ${fmtTime(latest.observed_at)}`;

    humidityValue.textContent = formatPct(latest.humidity);
    humiditySub.textContent = latest.observed_at ? `@ ${fmtTime(latest.observed_at)}` : "Latest reading";
    windValue.textContent = formatWind(latest.wind_kph);
    windSub.textContent = latest.observed_at ? `Source: ${latest.source}` : "Latest reading";
    pressureValue.textContent = formatPressure(latest.pressure_hpa);
    rainValue.textContent = formatRain(latest.rain_mm);
    rainSub.textContent = latest.observed_at ? `@ ${fmtTime(latest.observed_at)}` : "Last observation";
}

function renderHourly(aggregates = []) {
    hourlyCards.innerHTML = "";
    if (!aggregates.length) {
        hourlyCards.innerHTML = '<div class="hour-card gradient-1">No data<span>--</span></div>';
        return;
    }

    aggregates.slice(0, 8).forEach((agg, idx) => {
        const card = document.createElement("div");
        card.className = `hour-card ${gradientClasses[idx % gradientClasses.length]}`;
        const timeLabel = fmtTime(agg.bucket_start);
        const tempLabel = formatTemp(agg.temp_avg ?? agg.temp_max ?? agg.temp_min);
        card.innerHTML = `${timeLabel}<span>${tempLabel}</span>`;
        hourlyCards.appendChild(card);
    });
}

function renderWeekly(forecast) {
    weeklyCards.innerHTML = "";
    const days = (forecast && forecast.daily) || [];
    if (!days.length) {
        weeklyCards.innerHTML = '<div class="day-card soft-1">No forecast<span>--</span></div>';
        return;
    }

    days.slice(0, 3).forEach((day, idx) => {
        const card = document.createElement("div");
        card.className = `day-card ${softClasses[idx % softClasses.length]}`;
        const dateLabel = new Date(day.date).toLocaleDateString([], { weekday: "short" });
        card.innerHTML = `${dateLabel}<span>${formatTemp(day.temp_high_c)} / ${formatTemp(day.temp_low_c)}</span>`;
        weeklyCards.appendChild(card);
    });
}

function renderAlerts(alerts = []) {
    currentAlerts = alerts;
    alertsList.innerHTML = "";
    if (!alerts.length) {
        alertsList.innerHTML = '<li class="alert-item">No alerts yet.</li>';
        return;
    }
    alerts.slice(0, 5).forEach((alert) => {
        const item = document.createElement("li");
        item.className = "alert-item";
        item.innerHTML = `<strong>[${alert.level}]</strong> ${alert.message} • ${fmtTime(alert.triggered_at)}`;
        alertsList.appendChild(item);
    });
}

function connectAlerts(cityId) {
    try {
        if (mqttClient) {
            mqttClient.end(true);
        }
        mqttClient = mqtt.connect(MQTT_URL);
        mqttClient.on("connect", () => {
            mqttClient.subscribe(`alerts/${cityId}`);
        });
        mqttClient.on("message", (_topic, message) => {
            try {
                const alert = JSON.parse(message.toString());
                renderAlerts([alert, ...(currentAlerts || [])].slice(0, 5));
            } catch (e) {
                console.warn("Failed to parse alert", e);
            }
        });
    } catch (e) {
        console.warn("MQTT connection failed", e);
    }
}

async function loadCity(cityLabel) {
    const cfg = CITY_CONFIG[cityLabel] || CITY_CONFIG.Brampton;
    cityName.textContent = cityLabel;

    const qs = `lat=${cfg.lat}&lon=${cfg.lon}`;
    const obsUrl = `${API_BASE}/cities/${cfg.id}/observations?limit=100`;
    const aggUrl = `${API_BASE}/cities/${cfg.id}/aggregates?window=${encodeURIComponent("15 minutes")}&limit=8`;
    const forecastUrl = `${API_BASE}/cities/${cfg.id}/forecast?${qs}`;
    const alertsUrl = `${API_BASE}/cities/${cfg.id}/alerts?limit=5`;

    try {
        const [observations, aggregates, forecast, alerts] = await Promise.all([
            fetchJSON(obsUrl),
            fetchJSON(aggUrl),
            fetchJSON(forecastUrl),
            fetchJSON(alertsUrl),
        ]);

        renderCurrent(observations);
        renderHourly(aggregates);
        renderWeekly(forecast);
        renderAlerts(alerts);
        connectAlerts(cfg.id);
    } catch (err) {
        console.error("Failed to load data", err);
        updatedTime.textContent = "Last Updated: error loading data";
        hourlyCards.innerHTML = '<div class="hour-card gradient-1">Error<span>--</span></div>';
        weeklyCards.innerHTML = '<div class="day-card soft-1">Error<span>--</span></div>';
        alertsList.innerHTML = `<li class="alert-item">Error: ${err.message}</li>`;
    }
}

citySelect.addEventListener("change", () => loadCity(citySelect.value));

// Initial load
loadCity(citySelect.value);
let mqttClient = null;
let currentAlerts = [];
