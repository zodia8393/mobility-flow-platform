const $ = (selector) => document.querySelector(selector);
const formatNumber = (value) => new Intl.NumberFormat("ko-KR").format(value ?? 0);
const formatDuration = (seconds) => seconds == null ? "—" : `${Number(seconds).toFixed(1)}s`;
const formatDateTime = (value) => value ? new Intl.DateTimeFormat("ko-KR", {
  month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit"
}).format(new Date(value)) : "—";
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
  if (level === "UNKNOWN") return "#94a3b8";
  return "#10b981";
}

function renderMap(segments) {
  const svg = $("#segment-map");
  const minLon = 126.77, maxLon = 127.13, minLat = 37.45, maxLat = 37.60;
  const x = (lon) => 35 + ((lon - minLon) / (maxLon - minLon)) * 690;
  const y = (lat) => 325 - ((lat - minLat) / (maxLat - minLat)) * 285;
  const grids = [120, 240, 360, 480, 600].map(v => `<line x1="${v}" y1="25" x2="${v}" y2="335" class="map-grid"/>`).join("")
    + [90, 180, 270].map(v => `<line x1="25" y1="${v}" x2="735" y2="${v}" class="map-grid"/>`).join("");
  const areaGroups = Object.groupBy(segments, segment => segment.area_name || "기타");
  const areaGuides = Object.entries(areaGroups).map(([areaName, items]) => {
    const points = items.flatMap(item => [
      [x(item.start_longitude ?? item.longitude), y(item.start_latitude ?? item.latitude)],
      [x(item.end_longitude ?? item.longitude), y(item.end_latitude ?? item.latitude)]
    ]);
    const xs = points.map(point => point[0]);
    const ys = points.map(point => point[1]);
    const minX = Math.max(24, Math.min(...xs) - 12);
    const minY = Math.max(24, Math.min(...ys) - 12);
    const maxX = Math.min(736, Math.max(...xs) + 12);
    const maxY = Math.min(336, Math.max(...ys) + 12);
    const labelWidth = Math.max(86, areaName.length * 11 + 42);
    const labelX = Math.min(minX, 730 - labelWidth);
    const labelY = Math.max(18, minY - 7);
    return `<g class="map-area-guide">
      <rect x="${minX}" y="${minY}" width="${Math.max(24, maxX - minX)}" height="${Math.max(24, maxY - minY)}" rx="10"/>
      <g transform="translate(${labelX} ${labelY})"><rect width="${labelWidth}" height="20" rx="10"/><text x="9" y="14">${areaName} · ${items.length}</text></g>
    </g>`;
  }).join("");
  const roads = segments.map((segment) => {
    const startX = x(segment.start_longitude ?? segment.longitude);
    const startY = y(segment.start_latitude ?? segment.latitude);
    const endX = x(segment.end_longitude ?? segment.longitude);
    const endY = y(segment.end_latitude ?? segment.latitude);
    const critical = ["SEVERE", "CONGESTED"].includes(segment.congestion_level);
    return `<line class="segment-line" x1="${startX}" y1="${startY}" x2="${endX}" y2="${endY}" stroke="${congestionColor(segment.congestion_level)}" stroke-width="${critical ? 5 : 3}">
      <title>${segment.area_name} · ${segment.road_name} · ${Number(segment.speed_kph).toFixed(1)} km/h · ${segment.congestion_level}</title></line>`;
  }).join("");
  svg.innerHTML = `<path d="M62 262 C140 240 150 176 232 181 S350 240 427 206 540 110 690 130" fill="none" stroke="#bfdbfe" stroke-width="13" opacity=".42"/>${grids}${areaGuides}${roads}`;
}

function renderAlerts(segments) {
  const risky = segments.filter(s => ["SEVERE", "CONGESTED", "SLOW"].includes(s.congestion_level))
    .sort((a, b) => Number(a.speed_kph) - Number(b.speed_kph)).slice(0, 18);
  $("#alerts").innerHTML = risky.length ? risky.map(s => {
    const action = s.congestion_level === "CONGESTED" ? "우회·지연 안내 검토" : "속도 추세 모니터링";
    const critical = ["SEVERE", "CONGESTED"].includes(s.congestion_level) ? "critical" : "";
    return `<div class="alert-item ${critical}"><i></i><div><b>${s.road_name}</b><small>${s.area_name} · ${action}</small></div><strong>${Number(s.speed_kph).toFixed(1)} km/h</strong></div>`;
  }).join("") : `<p class="empty">현재 점검이 필요한 정체 구간이 없습니다.</p>`;
}

function renderSource(source) {
  const areas = source.areas || [];
  $("#source-areas").textContent = areas.length ? `${areas.length}개 지역` : "수집 대기";
  $("#source-areas").title = areas.join(" · ");
  $("#source-records").textContent = `${formatNumber(source.unique_links)} links`;
  $("#source-snapshot").textContent = formatDateTime(source.snapshot_at);
  $("#source-license").textContent = "공공누리 1유형";
  $("#source-license").title = source.license;
  $("#source-link").href = source.source_url;
  $(".scope-badge").textContent = areas.length ? `LIVE · ${areas.length} AREAS` : "SEOUL OPEN DATA · WAITING";
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
    const [overview, segments, runs, services, delivery, source] = await Promise.all([
      fetchJson("/api/overview"), fetchJson("/api/segments"), fetchJson("/api/runs"), fetchJson("/api/services"), fetchJson("/api/delivery/latest"), fetchJson("/api/source")
    ]);
    $("#raw-rows").textContent = formatNumber(overview.raw_rows);
    $("#freshness").textContent = formatFreshness(overview.freshness_seconds);
    $("#segment-count").textContent = formatNumber(overview.segment_count);
    $("#area-count").textContent = `${formatNumber(overview.area_count)}개 지역`;
    $("#congested").textContent = formatNumber(overview.congested_segments);
    $("#dbt-status").textContent = overview.last_successful_run?.dbt_status || overview.last_run?.dbt_status || "WAITING";
    $("#platform-status").textContent = overview.platform_status;
    $("#last-run-id").textContent = overview.last_run?.run_id || "첫 실행을 기다리는 중";
    $("#last-refresh").textContent = `업데이트 ${new Date().toLocaleTimeString("ko-KR")}`;
    const pulse = $("#status-pulse");
    pulse.className = `pulse ${overview.platform_status === "HEALTHY" ? "healthy" : overview.platform_status === "FAILED" ? "failed" : ""}`;
    renderMap(segments); renderAlerts(segments); renderRuns(runs); renderServices(services); renderDelivery(delivery); renderSource(source);
  } catch (error) {
    $("#platform-status").textContent = "UNAVAILABLE";
    $("#last-run-id").textContent = error.message;
    $("#status-pulse").className = "pulse failed";
  }
}

refresh();
setInterval(refresh, 10000);
