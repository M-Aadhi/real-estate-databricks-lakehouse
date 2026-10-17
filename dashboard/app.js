/* Real Estate Portfolio Dashboard: renders Gold-layer metrics from dashboard_data.json. */
(function () {
  "use strict";

  const usd0 = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
  const num0 = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
  const money = (v) => (v == null ? "–" : usd0.format(v));
  const compact = (v) => {
    if (v == null) return "–";
    const a = Math.abs(v);
    if (a >= 1e9) return "$" + (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return "$" + (v / 1e6).toFixed(1) + "M";
    if (a >= 1e3) return "$" + (v / 1e3).toFixed(0) + "K";
    return usd0.format(v);
  };
  const pct = (v, d = 1) => (v == null ? "–" : Number(v).toFixed(d) + "%");
  const signedPct = (v) => (v == null ? "–" : (v > 0 ? "+" : v < 0 ? "−" : "") + Math.abs(v).toFixed(1) + "%");
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const shortName = (s, n = 22) => (s.length > n ? s.slice(0, n - 1) + "…" : s);
  const monthLabel = (ym) => {
    const [y, m] = ym.split("-").map(Number);
    return new Date(Date.UTC(y, m - 1, 1)).toLocaleString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
  };

  const charts = [];
  let DATA = null;

  function token(name) {
    return getComputedStyle(document.querySelector(".viz-root")).getPropertyValue(name).trim();
  }

  function baseOptions({ horizontal = false, money: isMoney = true, max } = {}) {
    const grid = token("--grid"), muted = token("--text-secondary");
    const valueAxis = {
      beginAtZero: true, max, grid: { color: grid, drawTicks: false }, border: { display: false },
      ticks: { color: muted, padding: 6, callback: (v) => (isMoney ? compact(v) : max === 100 ? v + "%" : num0.format(v)) },
    };
    const catAxis = { grid: { display: false }, border: { color: grid }, ticks: { color: muted, autoSkip: false } };
    return {
      responsive: true, maintainAspectRatio: false, indexAxis: horizontal ? "y" : "x",
      animation: { duration: 400 },
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: token("--surface-1"), titleColor: token("--text-primary"), bodyColor: token("--text-primary"),
          borderColor: token("--border"), borderWidth: 1, padding: 10, boxPadding: 4,
        },
      },
      scales: horizontal ? { x: valueAxis, y: catAxis } : { x: catAxis, y: valueAxis },
    };
  }

  const bar = (color) => ({ backgroundColor: color, borderRadius: 4, borderSkipped: "start", maxBarThickness: 22,
                            borderColor: token("--surface-1"), borderWidth: { top: 0, bottom: 0, left: 0, right: 0 } });

  function makeChart(id, config) {
    const el = document.getElementById(id);
    charts.push(new Chart(el, config));
  }

  function renderKpis(k) {
    const cards = [
      ["Portfolio value", compact(k.portfolio_value), `${signedPct(k.portfolio_value_growth_pct)} vs 12 months ago`],
      ["Properties", num0.format(k.total_properties), "in the portfolio"],
      ["Total units", num0.format(k.total_units), `${num0.format(k.occupied_units)} occupied · ${num0.format(k.vacant_units)} vacant`],
      ["Occupancy rate", pct(k.occupancy_rate), "units with an active lease"],
      ["Monthly rent", compact(k.scheduled_monthly_rent), `avg ${money(k.avg_rent_per_occupied_unit)} per occupied unit`],
      ["Collected rent", compact(k.collected_rent), `${pct(k.collection_rate)} of ${compact(k.billed_rent)} billed`],
      ["Outstanding rent", compact(k.outstanding_rent), `${num0.format(k.delinquent_tenants)} delinquent tenants`],
      ["Lease expirations", num0.format(k.leases_expiring_90_days), `next 90 days · ${num0.format(k.leases_expiring_30_days)} in 30 days`],
    ];
    document.getElementById("kpis").innerHTML = cards.map(([l, v, n]) =>
      `<div class="kpi"><div class="label">${esc(l)}</div><div class="value">${esc(v)}</div><div class="note">${esc(n)}</div></div>`).join("");
  }

  function renderCharts(d) {
    charts.splice(0).forEach((c) => c.destroy());
    const s1 = token("--series-1"), s2 = token("--series-2");

    const occ = d.occupancy_by_property;
    makeChart("occupancyChart", {
      type: "bar",
      data: { labels: occ.map((r) => shortName(r.property_name)),
              datasets: [{ label: "Occupancy", data: occ.map((r) => r.occupancy_rate), ...bar(s1) }] },
      options: (() => {
        const o = baseOptions({ horizontal: true, money: false, max: 100 });
        o.plugins.tooltip.callbacks = { label: (c) => ` ${pct(c.raw)} (${occ[c.dataIndex].occupied_units}/${occ[c.dataIndex].total_units} units)` };
        return o;
      })(),
    });

    const rev = d.revenue_by_property;
    makeChart("revenueChart", {
      type: "bar",
      data: { labels: rev.map((r) => shortName(r.property_name)),
              datasets: [{ label: "TTM collected rent", data: rev.map((r) => r.ttm_collected_rent), ...bar(s1) }] },
      options: (() => {
        const o = baseOptions({ horizontal: true });
        o.plugins.tooltip.callbacks = { label: (c) => ` ${money(c.raw)} · ${rev[c.dataIndex].property_type}` };
        return o;
      })(),
    });

    const vt = d.value_by_property_type;
    makeChart("valueChart", {
      type: "bar",
      data: { labels: vt.map((r) => r.property_type),
              datasets: [{ label: "Portfolio value", data: vt.map((r) => r.portfolio_value), ...bar(s1) }] },
      options: (() => {
        const o = baseOptions({ horizontal: true });
        o.plugins.tooltip.callbacks = { label: (c) => ` ${compact(c.raw)} · ${vt[c.dataIndex].properties} properties, ${num0.format(vt[c.dataIndex].units)} units` };
        return o;
      })(),
    });

    const ex = d.lease_expiry_timeline;
    makeChart("expiryChart", {
      type: "bar",
      data: { labels: ex.map((r) => monthLabel(r.expiry_month)),
              datasets: [{ label: "Leases expiring", data: ex.map((r) => r.leases), ...bar(s1) }] },
      options: (() => {
        const o = baseOptions({ money: false });
        o.plugins.tooltip.callbacks = { label: (c) => ` ${c.raw} leases · ${compact(ex[c.dataIndex].monthly_rent_at_risk)}/mo rent` };
        return o;
      })(),
    });

    const tr = d.collection_trend;
    makeChart("trendChart", {
      type: "bar",
      data: { labels: tr.map((r) => monthLabel(r.month)),
              datasets: [
                { label: "Billed", data: tr.map((r) => r.billed_rent), ...bar(s1) },
                { label: "Collected", data: tr.map((r) => r.collected_rent), ...bar(s2) },
              ] },
      options: (() => {
        const o = baseOptions();
        o.datasets = { bar: { categoryPercentage: 0.7, barPercentage: 0.9 } };
        o.plugins.legend = { display: true, position: "top", align: "end",
                             labels: { color: token("--text-secondary"), boxWidth: 10, boxHeight: 10, useBorderRadius: true, borderRadius: 2 } };
        o.plugins.tooltip.callbacks = {
          label: (c) => ` ${c.dataset.label}: ${money(c.raw)}`,
          footer: (items) => ` Collection rate: ${pct(tr[items[0].dataIndex].collection_rate)}`,
        };
        return o;
      })(),
    });

    const mt = d.maintenance_by_property;
    makeChart("maintChart", {
      type: "bar",
      data: { labels: mt.map((r) => shortName(r.property_name)),
              datasets: [{ label: "TTM maintenance cost", data: mt.map((r) => r.ttm_maintenance_cost), ...bar(s2) }] },
      options: (() => {
        const o = baseOptions({ horizontal: true });
        o.plugins.tooltip.callbacks = { label: (c) => ` ${money(c.raw)} · ${mt[c.dataIndex].work_orders} work orders` };
        return o;
      })(),
    });
  }

  function renderTables(d) {
    const tbody = document.querySelector("#propTable tbody");
    const rows = d.properties.map((p) => ({
      key: `${p.property_name} ${p.property_type} ${p.city}`.toLowerCase(),
      html: `<tr><td>${esc(p.property_name)}</td><td>${esc(p.property_type)}</td><td>${esc(p.city)}</td>
        <td class="num">${num0.format(p.total_units)}</td><td class="num">${pct(p.occupancy_rate)}</td>
        <td class="num">${money(p.scheduled_monthly_rent)}</td><td class="num">${pct(p.collection_rate)}</td>
        <td class="num">${money(p.ttm_collected_rent)}</td><td class="num">${money(p.ttm_maintenance_cost)}</td>
        <td class="num">${compact(p.current_value)}</td>
        <td class="num ${p.value_growth_pct < 0 ? "neg" : "pos"}">${signedPct(p.value_growth_pct)}</td></tr>`,
    }));
    const draw = (q) => { tbody.innerHTML = rows.filter((r) => !q || r.key.includes(q)).map((r) => r.html).join(""); };
    draw("");
    document.getElementById("search").addEventListener("input", (e) => draw(e.target.value.trim().toLowerCase()));

    document.querySelector("#delTable tbody").innerHTML = d.top_delinquent_tenants.map((t) =>
      `<tr><td>${esc(t.tenant_name)}</td><td>${esc(t.property_name)}</td><td class="num">${money(t.outstanding_balance)}</td>
       <td class="num">${t.days_past_due}</td><td>${esc(t.aging_bucket)} days</td></tr>`).join("")
      || `<tr><td colspan="5">No delinquent tenants.</td></tr>`;
  }

  function render(d) {
    DATA = d;
    document.getElementById("title").textContent = d.meta.title;
    document.getElementById("meta").textContent =
      `As of ${d.meta.as_of_date} · reporting month ${d.meta.reporting_month} · ${d.kpis.total_properties} properties`;
    const note = document.getElementById("note");
    note.textContent = d.meta.data_note;
    note.hidden = false;
    document.getElementById("foot").textContent =
      `Source: ${d.meta.source}. Generated ${d.meta.generated_at}. Occupancy is derived from active leases; ` +
      `"Estimated NOI" in the data uses a documented operating-expense assumption and is not an audited NOI.`;
    renderKpis(d.kpis);
    renderCharts(d);
    renderTables(d);
  }

  function showError(msg) {
    const el = document.getElementById("error");
    el.textContent = msg;
    el.hidden = false;
    document.getElementById("meta").textContent = "No data loaded";
  }

  document.getElementById("theme").addEventListener("click", () => {
    const root = document.documentElement;
    const dark = root.dataset.theme ? root.dataset.theme === "dark"
      : window.matchMedia("(prefers-color-scheme: dark)").matches;
    root.dataset.theme = dark ? "light" : "dark";
    if (DATA) renderCharts(DATA);
  });

  if (typeof Chart === "undefined") {
    showError("Chart.js failed to load (dashboard/vendor/chart.umd.js missing).");
    return;
  }
  Chart.defaults.font.family = getComputedStyle(document.querySelector(".viz-root")).fontFamily;

  if (window.DASHBOARD_DATA) {
    render(window.DASHBOARD_DATA);
  } else {
    fetch("data/dashboard_data.json")
      .then((r) => { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(render)
      .catch(() => showError("Could not load data/dashboard_data.json. Run: python -m src.dashboard.export_dashboard"));
  }
})();
