const $ = (selector) => document.querySelector(selector);
const formatNumber = (value) => new Intl.NumberFormat("ko-KR").format(value ?? 0);
const formatDuration = (seconds) => seconds == null ? "—" : `${Number(seconds).toFixed(1)}s`;
const formatFreshness = (seconds) => {
  if (seconds == null) return "—";
  if (seconds < 60) return `${Math.round(seconds)}초`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}분`;
  return `${(seconds / 3600).toFixed(1)}시간`;
};

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return response.json();
}

function congestionColor(level) {
  if (["SEVERE", "CONGESTED"].includes(level)) return "#ef4444";
  if (level === "SLOW") return "#f59e0b";
  return "#10b981";
}

function renderMap(segments) {
  const svg = $("#segment-map");
  const minLon = 126.77, maxLon = 127.13, minLat = 37.45, maxLat = 37.60;
  const x = (lon) => 35 + ((lon - minLon) / (maxLon - minLon)) * 690;
  const y = (lat) => 325 - ((lat - minLat) / (maxLat - minLat)) * 285;
  const grids = [120, 240, 360, 480, 600].map(v => `<line x1="${v}" y1="25" x2="${v}" y2="335" class="map-grid"/>`).join("")
    + [90, 180, 270].map(v => `<line x1="25" y1="${v}" x2="735" y2="${v}" class="map-grid"/>`).join("");
  const roads = segments.map((segment, index) => {
    const cx = x(segment.longitude), cy = y(segment.latitude);
    const critical = ["SEVERE", "CONGESTED"].includes(segment.congestion_level);
    const slope = index % 2 === 0 ? 7 : -7;
    return `<g><line class="segment-line" x1="${cx - 17}" y1="${cy - slope}" x2="${cx + 17}" y2="${cy + slope}" stroke="${congestionColor(segment.congestion_level)}" stroke-width="${critical ? 10 : 7}">
      <title>${segment.road_name}: ${Number(segment.speed_kph).toFixed(1)} / ${Number(segment.reference_speed_kph).toFixed(0)} km/h · ${segment.congestion_level} · ${segment.source_system}</title></line>
      ${critical ? `<text class="segment-label" x="${cx + 15}" y="${cy - 10}">${segment.road_name}</text>` : ""}</g>`;
  }).join("");
  svg.innerHTML = `<path d="M62 262 C140 240 150 176 232 181 S350 240 427 206 540 110 690 130" fill="none" stroke="#bfdbfe" stroke-width="13" opacity=".55"/>${grids}${roads}`;
}

function renderAlerts(segments) {
  const risky = segments.filter(s => ["SEVERE", "CONGESTED", "SLOW"].includes(s.congestion_level))
    .sort((a, b) => a.speed_index - b.speed_index);
  $("#alerts").innerHTML = risky.length ? risky.map(s => {
    const action = s.congestion_level === "SEVERE" ? "원천·집계 즉시 확인" : s.congestion_level === "CONGESTED" ? "혼잡 구간 점검" : "추세 모니터링";
    const critical = ["SEVERE", "CONGESTED"].includes(s.congestion_level) ? "critical" : "";
    return `<div class="alert-item ${critical}"><i></i><div><b>${s.road_name}</b><small>${action} · ${s.source_system} · ${s.congestion_level}</small></div><strong>${Number(s.speed_kph).toFixed(1)} km/h</strong></div>`;
  }).join("") : `<p class="empty">현재 점검이 필요한 정체 구간이 없습니다.</p>`;
}

function renderRuns(runs) {
  $("#runs").innerHTML = runs.length ? runs.map(run => `<tr>
    <td title="${run.run_id}">${run.run_id.slice(0, 22)}</td>
    <td><span class="status-label ${run.status.toLowerCase()}">${run.status}</span></td>
    <td>${formatNumber(run.input_rows)}</td><td>${formatNumber(run.accepted_rows)}</td>
    <td>${formatNumber(run.duplicates_removed)}</td><td>${formatNumber(run.late_rows)}</td>
    <td>${formatDuration(run.job_duration_seconds)}</td><td>${run.dbt_status || "—"}</td>
  </tr>`).join("") : `<tr><td colspan="8">실행 이력이 없습니다.</td></tr>`;
}

function renderServices(services) {
  $("#service-summary").innerHTML = Object.entries(services).map(([name, service]) =>
    `<span class="service-pill ${service.status.toLowerCase()}">${name.toUpperCase()} · ${service.status}</span>`
  ).join("");
  document.querySelectorAll("[data-service]").forEach(node => {
    const service = services[node.dataset.service];
    if (service) node.classList.add(service.status.toLowerCase());
  });
}

function renderDelivery(delivery) {
  $("#delivery-status").textContent = delivery.delivery_status;
  $("#delivery-status").className = delivery.delivery_status.toLowerCase();
  $("#delivery-run").textContent = delivery.run_id || "성공 run 대기";
  $("#delivery-checks").innerHTML = delivery.checks.length ? delivery.checks.map(check =>
    `<div class="delivery-check ${check.status.toLowerCase()}"><i>${check.status === "PASS" ? "✓" : "!"}</i><span><b>${check.name}</b><small>${check.value}</small></span></div>`
  ).join("") : `<span class="empty">첫 성공 run 이후 row·dbt·hash gate가 표시됩니다.</span>`;
}

async function refresh() {
  try {
    const [overview, segments, runs, services, delivery] = await Promise.all([
      fetchJson("/api/overview"), fetchJson("/api/segments"), fetchJson("/api/runs"), fetchJson("/api/services"), fetchJson("/api/delivery/latest")
    ]);
    $("#raw-rows").textContent = formatNumber(overview.raw_rows);
    $("#freshness").textContent = formatFreshness(overview.freshness_seconds);
    $("#segment-count").textContent = formatNumber(overview.segment_count);
    $("#congested").textContent = formatNumber(overview.congested_segments);
    $("#dbt-status").textContent = overview.last_successful_run?.dbt_status || overview.last_run?.dbt_status || "WAITING";
    $("#platform-status").textContent = overview.platform_status;
    $("#last-run-id").textContent = overview.last_run?.run_id || "첫 실행을 기다리는 중";
    $("#last-refresh").textContent = `업데이트 ${new Date().toLocaleTimeString("ko-KR")}`;
    const pulse = $("#status-pulse");
    pulse.className = `pulse ${overview.platform_status === "HEALTHY" ? "healthy" : overview.platform_status === "FAILED" ? "failed" : ""}`;
    renderMap(segments); renderAlerts(segments); renderRuns(runs); renderServices(services); renderDelivery(delivery);
  } catch (error) {
    $("#platform-status").textContent = "UNAVAILABLE";
    $("#last-run-id").textContent = error.message;
    $("#status-pulse").className = "pulse failed";
  }
}

refresh();
setInterval(refresh, 10000);
