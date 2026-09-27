import { FlowerClient } from "../../sdk/index.ts";
import type pizza from "../goblin-pizza-ts/goblin-pizza.ts";
import type { PizzaDashboard, PizzaOrder } from "../goblin-pizza-ts/goblin-pizza.ts";

// The launcher's own view of the whole demo: its load, the replicas' Raft
// metrics and the latency of the calls it made. The database never sees this.
interface DemoStats {
  rate: number; maxRate: number; paused: boolean; maxOrders: number; sequence: number; pendingOrders: number; drones: number;
  counters: { placed: number; delivered: number; pizzas: number; tips: number; archived: number; walkedAway: number; lostDrones: number; reclaimed: number; errors: number };
  latency: { windowMs: number; calls: number; p50: number | null; p99: number | null };
  nodes: { id: number; running: boolean; state: string; applied: number | null; term: number | null; streams: number }[];
  recovering: boolean;
}
interface Sample { at: number; placed: number; delivered: number }
interface Point { at: number; placed: number; delivered: number }

const client = new FlowerClient<typeof pizza>(window.location.origin);
const lifetime = new AbortController();
const number = new Intl.NumberFormat();
const decimal = new Intl.NumberFormat(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const colors = ["#eab983", "#bdcca1", "#dec68d", "#d5b9a3"];
const symbols = ["✳", "✿", "❋", "✦"];
const chartWindowMs = 120_000;
let value: PizzaDashboard | undefined;
let revision = 0;
let received = 0;
let lastChange = 0;
let connected = false;
let paused = document.querySelector<HTMLMetaElement>('meta[name="pizza-demo-paused"]')?.content === "true";
let filter = "all";
let shopIds = "";
let actionPending = false;
let selectedTenant = "tenant-0";
let watchController: AbortController | undefined;
let stats: DemoStats | undefined;
let samples: Sample[] = [];
let failovers: { at: number; until: number | null }[] = [];
let hover: number | null = null;
// Recent tenant totals, for live rates on the tiles and kitchen cards.
let history: { at: number; totals: PizzaDashboard["totals"]; orders: Record<string, number> }[] = [];
let updateTimes: number[] = [];
let renderQueued = false;
let lastRender = 0;

function element<T extends HTMLElement = HTMLElement>(id: string): T {
  const node = document.getElementById(id);
  if (!node) throw new Error(`Missing dashboard element ${id}`);
  return node as T;
}
function text(id: string, value: string | number): void { element(id).textContent = String(value); }
function escape(value: unknown): string {
  return String(value).replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]!);
}
function shortId(id: string): string { return id.length > 20 ? `${id.slice(0, 9)}…${id.slice(-8)}` : id; }
// A countdown the local clock keeps ticking; `due` shows once it runs out and
// until the replica's next value says what happened.
function deadline(time: number, due: string): string {
  return `<span data-deadline="${time}" data-due="${escape(due)}">~${Math.max(0, (time - Date.now()) / 1000).toFixed(1)}s</span>`;
}
// Orders, jobs and timers are keyed by canonical JSON [tenant, store, order].
function orderOf(key: string): string {
  try { return String(JSON.parse(key).at(-1)); } catch { return shortId(key); }
}
const activity = (order: PizzaOrder) => order.deliveredAt ?? order.readyAt ?? order.createdAt;
function elapsed(from: number, until: number): string { return `${Math.max(0, (until - from) / 1000).toFixed(1)}s total`; }
function shopName(id: string): string { return value?.summaries[id]?.name ?? id; }
function perSecond(rate: number | null): string { return rate === null ? "—" : rate >= 10 ? number.format(Math.round(rate)) : decimal.format(rate); }
function status(state: "connecting" | "connected" | "disconnected", message?: string): void {
  connected = state === "connected";
  element("connection").className = `connection ${state}`;
  text("connection-text", message ?? (connected ? "Replica stream connected" : state === "connecting" ? "Connecting" : "Reconnecting"));
  element("stale-banner").hidden = connected || !value;
  document.body.classList.toggle("stale-data", !connected && Boolean(value));
  updateControls();
}
function updateControls(): void {
  for (const id of ["order", "tip"]) element<HTMLButtonElement>(id).disabled = !connected || !value || value.tenant !== selectedTenant || actionPending;
  element<HTMLSelectElement>("shop").disabled = !value || value.tenant !== selectedTenant || actionPending;
  element<HTMLSelectElement>("tenant").disabled = !value || actionPending;
  element<HTMLButtonElement>("pause").disabled = actionPending;
  element<HTMLButtonElement>("crash").disabled = actionPending || Boolean(stats?.recovering);
  for (const button of document.querySelectorAll<HTMLButtonElement>("[data-rate]")) {
    button.disabled = actionPending || !stats;
    button.setAttribute("aria-pressed", String(stats?.rate === Number(button.dataset.rate)));
  }
}
function showPaused(): void {
  element("pause").setAttribute("aria-pressed", String(paused));
  element("pause").innerHTML = paused ? 'Resume arrivals <span aria-hidden="true">▷</span>' : 'Pause arrivals <span aria-hidden="true">Ⅱ</span>';
}

// Per-second rate of a counter over the recent tenant history.
function rate(pick: (entry: typeof history[number]) => number): number | null {
  const latest = history.at(-1);
  const oldest = history.find((entry) => latest && latest.at - entry.at <= 6_000);
  if (!latest || !oldest || latest.at - oldest.at < 2_000) return null;
  return Math.max(0, (pick(latest) - pick(oldest)) * 1_000 / (latest.at - oldest.at));
}
function remember(update: PizzaDashboard): void {
  const now = Date.now();
  if (update.tenant !== value?.tenant) history = [];
  history.push({ at: now, totals: update.totals, orders: Object.fromEntries(Object.entries(update.summaries).map(([key, shop]) => [key, shop.orders])) });
  while (history.length > 2 && now - history[1].at > 6_000) history.shift();
  updateTimes.push(now);
  while (updateTimes.length && now - updateTimes[0] > 5_000) updateTimes.shift();
}

function renderOrders(): void {
  if (!value) return;
  // Latest activity first: fresh orders, pizzas out of the oven and doorstep
  // arrivals interleave, so every stage stays in view at any pace.
  const recent = Object.values(value.orders).sort((a, b) => activity(b) - activity(a) || a.id.localeCompare(b.id));
  const visible = recent.filter((order) => filter === "all" || order.status === filter).slice(0, 12);
  element("orders").innerHTML = visible.length ? visible.map((order: PizzaOrder) => {
    const job = value!.jobs[order.key];
    const timing = order.status === "baking" ? deadline(order.dueAt, "Out any moment") : order.deliveredAt !== null
      ? elapsed(order.createdAt, order.deliveredAt) : job?.lease ? deadline(job.lease.expiresAt, "Lease lapsing") : "Awaiting drone";
    return `<tr><td class="order-id" title="${escape(order.key)}">${escape(shortId(order.id))}<small>${escape(shopName(JSON.stringify(order.shop)))}</small></td><td>${number.format(order.quantity)} <span aria-hidden="true">↗</span></td><td><span class="status-badge ${order.status}">${order.status === "baking" ? "Baking" : order.status === "ready" ? job?.lease ? "In flight" : "Ready" : "Delivered"}</span></td><td class="timing">${timing}</td></tr>`;
  }).join("") : `<tr><td colspan="4" class="empty">${filter === "all" ? "No orders yet. The ovens are ready when you are." : `No ${escape(filter)} orders in the recent window.`}</td></tr>`;
  text("orders-foot", `Showing ${visible.length} of the ${recent.length} orders with the latest activity · delivered orders are archived into kitchen tallies after ten seconds`);
}
function render(): void {
  if (!value) return;
  const totals = value.totals;
  text("pizzas", number.format(totals.pizzas));
  text("in-flight", number.format(totals.baking + totals.ready));
  text("revenue", number.format(totals.revenue));
  text("tips", number.format(totals.tips));
  const pizzaRate = rate((entry) => entry.totals.pizzas);
  text("pizzas-rate", pizzaRate === null ? "doorsteps made happier" : `+${perSecond(pizzaRate)} a second`);
  const revenueRate = rate((entry) => entry.totals.revenue);
  text("revenue-rate", revenueRate === null ? "from completed deliveries" : `+${perSecond(revenueRate)} a second`);
  const tipRate = rate((entry) => entry.totals.tips);
  text("tips-rate", tipRate === null || tipRate === 0 ? "a little extra appreciation" : `+${perSecond(tipRate)} a second`);
  text("total-orders", `${value.tenant} · ${number.format(totals.orders)} orders through the kitchens · ${number.format(value.archived)} archived`);
  const tenantSelector = element<HTMLSelectElement>("tenant");
  if (JSON.stringify([...tenantSelector.options].map((option) => option.value)) !== JSON.stringify(value.tenantIds)) {
    tenantSelector.innerHTML = value.tenantIds.map((tenant) => `<option value="${escape(tenant)}">${escape(tenant)}</option>`).join("");
  }
  tenantSelector.value = selectedTenant;
  for (const state of ["baking", "ready", "delivered"] as const) {
    text(state, number.format(totals[state]));
    element(`${state}-bar`).style.width = `${100 * totals[state] / Math.max(1, totals.orders)}%`;
  }
  // Oven to doorstep, over the deliveries still on the board.
  const trips = Object.values(value.orders).filter((order) => order.deliveredAt !== null)
    .map((order) => order.deliveredAt! - order.createdAt).sort((a, b) => a - b);
  text("trip", trips.length ? `Oven to doorstep in ${decimal.format(trips[Math.floor(trips.length / 2)] / 1000)} s (median of ${trips.length} recent deliveries)` : "Waiting for the first delivery");
  const identities = JSON.stringify(value.config.shopIds);
  if (identities !== shopIds) {
    const selector = element<HTMLSelectElement>("shop");
    const previous = selector.value;
    const keys = value.config.shopIds.map((id) => JSON.stringify(id));
    selector.innerHTML = keys.map((id) => `<option value="${escape(id)}">${escape(shopName(id))}</option>`).join("");
    if (keys.includes(previous)) selector.value = previous;
    shopIds = identities;
  }
  element("shops").innerHTML = value.leaderboard.map((id, rank) => {
    const shop = value!.summaries[id];
    if (!shop) return "";
    const index = value!.config.shopIds.findIndex((ref) => JSON.stringify(ref) === id);
    const orderRate = rate((entry) => entry.orders[id] ?? 0);
    return `<article class="shop-card" style="--shop-color:${colors[index % colors.length]}"><div class="shop-top"><span class="shop-icon" aria-hidden="true">${symbols[index % symbols.length]}</span><div><h3>${escape(shop.name)}</h3><p>${orderRate === null ? "" : `${perSecond(orderRate)} orders/s · `}${number.format(shop.baking)} baking · ${number.format(shop.ready)} ready to fly</p></div><span class="rank">#${rank + 1}</span></div><div class="shop-money"><strong>${number.format(shop.revenue)} <small>copper</small></strong><small>${number.format(shop.delivered)} delivered</small></div><div class="stock-bar" aria-hidden="true"><i style="width:${Math.max(0, Math.min(100, 100 * shop.stock / Math.max(1, shop.initialStock)))}%"></i></div><div class="stock-label"><span>Dough in the pantry</span><span>${number.format(shop.stock)} / ${number.format(shop.initialStock)}</span></div></article>`;
  }).join("");
  renderOrders();
  const jobs = Object.values(value.jobs);
  const leased = jobs.filter((job) => job.state === "leased" && job.lease).sort((a, b) => a.lease!.expiresAt - b.lease!.expiresAt);
  text("lease-count", leased.length);
  element("leases").innerHTML = leased.length ? leased.slice(0, 5).map((job) => `<div class="dispatch-row"><div><strong>${escape(job.lease!.owner)}</strong>${deadline(job.lease!.expiresAt, "Lease lapsing")}</div><p>${escape(job.payload.orderId)} · ${escape(job.payload.shop[1])} · fence #${job.lease!.token} · attempt ${job.attempts}</p></div>`).join("") : '<p class="empty">All drones are back at the roost.<br>The next delivery is on its way.</p>';
  const pending = jobs.filter((job) => job.state === "pending").length;
  text("queue-count", `${pending} waiting for pickup · ${leased.length} active leases, each fenced by its token`);
  const timers = Object.values(value.timers).sort((a, b) => a.dueAt - b.dueAt || a.id.localeCompare(b.id));
  text("timer-count", timers.length);
  element("timers").innerHTML = timers.length ? timers.slice(0, 5).map((timer) => `<div class="dispatch-row"><div><strong>${escape(orderOf(timer.id.replace(/^bake:/, "")))}</strong>${timer.state === "failed" ? '<span>Needs attention</span>' : deadline(timer.dueAt, "Ringing…")}</div><p>${escape(timer.handler)} · ${timer.attempts ? `${timer.attempts} attempts` : "waiting for the bell"}</p></div>`).join("") : '<p class="empty">The ovens have a moment to breathe.<br>Place an order to put them back to work.</p>';
  updateControls();
  tick();
}
// Values can arrive faster than anyone reads; paint at most ten times a second.
function schedule(): void {
  if (renderQueued) return;
  renderQueued = true;
  const wait = Math.max(0, 100 - (performance.now() - lastRender));
  setTimeout(() => requestAnimationFrame(() => {
    renderQueued = false;
    lastRender = performance.now();
    render();
  }), wait);
}
function tick(): void {
  for (const node of document.querySelectorAll<HTMLElement>("[data-deadline]")) {
    const remaining = Number(node.dataset.deadline) - Date.now();
    node.textContent = remaining > 0 ? `~${(remaining / 1000).toFixed(1)}s` : node.dataset.due ?? "Due";
  }
  const updatesPerSecond = updateTimes.length > 1 ? (updateTimes.length - 1) * 1_000 / Math.max(1, Date.now() - updateTimes[0]) : 0;
  if (value) text("stream-detail", `Revision ${number.format(revision)} · ${number.format(received)} values received · ${decimal.format(updatesPerSecond)} a second · last change ${Math.max(0, Math.floor((Date.now() - lastChange) / 1000))}s ago`);
}

// Throughput across every tenant, from the launcher's acknowledged calls.
function points(): Point[] {
  const result: Point[] = [];
  for (let index = 1; index < samples.length; index++) {
    // A three-second trailing window steadies Poisson arrivals without lagging much.
    const base = samples[Math.max(0, index - 3)];
    const sample = samples[index];
    const seconds = (sample.at - base.at) / 1_000;
    if (seconds > 0) result.push({ at: sample.at, placed: (sample.placed - base.placed) / seconds, delivered: (sample.delivered - base.delivered) / seconds });
  }
  return result;
}
function niceStep(maximum: number): number {
  const rough = maximum / 4;
  const magnitude = 10 ** Math.floor(Math.log10(Math.max(rough, 1e-9)));
  return [1, 2, 2.5, 5, 10].map((step) => step * magnitude).find((step) => step >= rough) ?? magnitude * 10;
}
function renderChart(): void {
  const svg = element<HTMLElement>("chart");
  const width = Math.max(280, svg.clientWidth);
  const height = Math.max(200, svg.clientHeight);
  const pad = { left: 38, right: 14, top: 12, bottom: 26 };
  const now = samples.at(-1)?.at ?? Date.now();
  const data = points();
  const top = Math.max(5, ...data.flatMap((point) => [point.placed, point.delivered]), (stats?.rate ?? 0) * 1.1);
  const step = niceStep(top);
  const yMax = Math.ceil(top / step) * step;
  const x = (at: number) => pad.left + (width - pad.left - pad.right) * (1 - (now - at) / chartWindowMs);
  const y = (count: number) => pad.top + (height - pad.top - pad.bottom) * (1 - count / yMax);
  const grid: string[] = [];
  for (let tick = 0; tick <= yMax + 1e-9; tick += step) {
    grid.push(`<line class="grid" x1="${pad.left}" x2="${width - pad.right}" y1="${y(tick)}" y2="${y(tick)}"/><text class="axis" x="${pad.left - 8}" y="${y(tick) + 3}" text-anchor="end">${number.format(tick)}</text>`);
  }
  for (const seconds of [120, 90, 60, 30, 0]) {
    grid.push(`<text class="axis" x="${x(now - seconds * 1_000)}" y="${height - 8}" text-anchor="${seconds === 120 ? "start" : seconds === 0 ? "end" : "middle"}">${seconds ? `${seconds}s ago` : "now"}</text>`);
  }
  const marks = failovers.filter((failover) => now - failover.at < chartWindowMs).map((failover) => {
    const left = x(failover.at);
    const right = x(failover.until ?? now);
    // Keep the label inside the plot while the band is still at its right edge.
    const label = left > width - pad.right - 80 ? `x="${left - 4}" text-anchor="end"` : `x="${left + 4}"`;
    return `<rect class="failover" x="${left}" y="${pad.top}" width="${Math.max(2, right - left)}" height="${height - pad.top - pad.bottom}"/><text class="axis failover-label" ${label} y="${pad.top + 11}">leader killed</text>`;
  });
  const visible = data.filter((point) => now - point.at <= chartWindowMs);
  const line = (key: "placed" | "delivered") => visible.map((point, index) => `${index ? "L" : "M"}${x(point.at).toFixed(1)},${y(point[key]).toFixed(1)}`).join("");
  const last = visible.at(-1);
  const selected = hover === null ? undefined : visible.reduce<Point | undefined>((best, point) =>
    !best || Math.abs(x(point.at) - hover!) < Math.abs(x(best.at) - hover!) ? point : best, undefined);
  const dots = (point: Point | undefined) => point ? `<circle class="dot placed" cx="${x(point.at)}" cy="${y(point.placed)}" r="4"/><circle class="dot delivered" cx="${x(point.at)}" cy="${y(point.delivered)}" r="4"/>` : "";
  svg.innerHTML = `<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="Orders placed and delivered per second over the last two minutes">${grid.join("")}${marks.join("")}` +
    `<path class="series placed" d="${line("placed")}"/><path class="series delivered" d="${line("delivered")}"/>` +
    (selected ? `<line class="crosshair" x1="${x(selected.at)}" x2="${x(selected.at)}" y1="${pad.top}" y2="${height - pad.bottom}"/>${dots(selected)}` : dots(last)) + "</svg>";
  text("placed-rate", last ? `${perSecond(last.placed)}/s` : "—");
  text("delivered-rate", last ? `${perSecond(last.delivered)}/s` : "—");
  const tooltip = element("chart-tooltip");
  tooltip.hidden = !selected;
  if (selected) {
    const ago = Math.round((now - selected.at) / 1_000);
    tooltip.replaceChildren();
    const heading = document.createElement("p");
    heading.textContent = ago ? `${ago}s ago` : "Now";
    tooltip.append(heading);
    for (const [key, label] of [["placed", "orders placed"], ["delivered", "orders delivered"]] as const) {
      const row = document.createElement("div");
      const swatch = document.createElement("i");
      swatch.className = key;
      const strong = document.createElement("strong");
      strong.textContent = `${perSecond(selected[key])}/s`;
      const name = document.createElement("span");
      name.textContent = label;
      row.append(swatch, strong, name);
      tooltip.append(row);
    }
    const left = x(selected.at);
    tooltip.style.left = `${Math.min(width - 150, Math.max(0, left + 12))}px`;
  }
}
function renderCluster(): void {
  if (!stats) return;
  const leader = stats.nodes.find((node) => node.state === "Leader");
  element("nodes").innerHTML = stats.nodes.map((node) => {
    const role = !node.running ? "Stopped" : node.state === "Leader" ? "Leader" : node.state === "Follower" ? "Follower" : node.state;
    const behind = leader?.applied != null && node.applied != null && node !== leader ? leader.applied - node.applied : 0;
    const detail = !node.running ? "Restarting from its data directory soon" : [
      node.applied === null ? "Waiting for metrics" : `applied #${number.format(node.applied)}${behind > 0 ? ` · ${number.format(behind)} behind` : ""}`,
      node.term === null ? "" : `term ${node.term}`,
      node.streams ? `${node.streams} dashboard ${node.streams === 1 ? "stream" : "streams"}` : "",
    ].filter(Boolean).join(" · ");
    return `<div class="node ${role.toLowerCase()}"><i aria-hidden="true"></i><div><strong>Replica ${node.id}</strong><p>${escape(detail)}</p></div><span>${escape(role)}</span></div>`;
  }).join("");
  const { latency, counters } = stats;
  text("latency-p50", latency.p50 === null ? "—" : `${number.format(Math.round(latency.p50))} ms`);
  text("latency-p99", latency.p99 === null ? "—" : `${number.format(Math.round(latency.p99))} ms`);
  text("latency-calls", `${number.format(latency.calls)} writes in the last ${latency.windowMs / 1000} s, from call to durable commit on a quorum`);
  text("fleet", `${number.format(stats.drones)} drones · ${number.format(counters.archived)} orders archived · ${number.format(counters.lostDrones)} drones lost, ${number.format(counters.reclaimed)} deliveries reclaimed${counters.walkedAway ? ` · ${number.format(counters.walkedAway)} customers walked away` : ""}`);
  if (stats.paused !== paused) { paused = stats.paused; showPaused(); }
  if (stats.sequence >= stats.maxOrders && !actionPending) {
    text("action-status", `The kitchens closed after ${number.format(stats.maxOrders)} orders to keep memory in check. Restart bin/demo for a fresh one.`);
  }
  updateControls();
}
async function pollStats(): Promise<void> {
  while (!lifetime.signal.aborted) {
    try {
      const response = await fetch("/demo/stats", { signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(5_000)]) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const next = await response.json() as DemoStats;
      const now = Date.now();
      if (next.recovering && !stats?.recovering) failovers.push({ at: now, until: null });
      if (!next.recovering) for (const failover of failovers) failover.until ??= now;
      stats = next;
      samples.push({ at: now, placed: next.counters.placed, delivered: next.counters.delivered });
      while (samples.length > 2 && now - samples[1].at > chartWindowMs + 4_000) samples.shift();
      failovers = failovers.filter((failover) => now - (failover.until ?? now) < chartWindowMs);
      element("pulse").classList.remove("stale");
      renderCluster();
      renderChart();
    } catch {
      if (lifetime.signal.aborted) return;
      element("pulse").classList.add("stale");
    }
    await wait(1_000);
  }
}

async function action(action: "order" | "tip" | "pause" | "crash" | "rate", rate?: number): Promise<void> {
  if (actionPending) return;
  actionPending = true;
  updateControls();
  element("action-status").classList.remove("error");
  text("action-status", action === "crash" ? "Stopping the leader. The other replicas are choosing a replacement…" : action === "rate" ? "Telling the customers…" : "Sending it to the kitchens…");
  try {
    const response = await fetch("/demo/action", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, ...(["order", "tip"].includes(action) ? { shop: JSON.parse(element<HTMLSelectElement>("shop").value) } : {}), ...(rate === undefined ? {} : { rate }) }),
      signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(40_000)]),
    });
    const result = await response.json() as { ok: boolean; paused?: boolean; rate?: number; error?: string | { message?: string } };
    if (!response.ok || !result.ok) throw new Error(typeof result.error === "string" ? result.error : result.error?.message ?? "The kitchen could not accept that action.");
    if (typeof result.paused === "boolean" || action === "pause") {
      paused = result.paused ?? !paused;
      showPaused();
    }
    if (stats && typeof result.rate === "number") stats.rate = result.rate;
    text("action-status", action === "order" ? "Order accepted. It will appear when this replica catches up." : action === "tip" ? "Three copper coins for the goblins. The board will catch up."
      : action === "pause" ? paused ? "Automatic arrivals paused. The drones are still delivering." : "Automatic arrivals resumed. Here comes the next rush."
      : action === "rate" ? `About ${result.rate} orders a second now. The drone fleet resizes to match.`
      : "A new leader is serving and the old one caught up. The board streams from an available replica.");
  } catch (error) {
    if (!lifetime.signal.aborted) {
      element("action-status").classList.add("error");
      text("action-status", error instanceof Error ? error.message : "Action failed. Please try again.");
    }
  } finally { actionPending = false; updateControls(); }
}
function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const done = () => { clearTimeout(timer); lifetime.signal.removeEventListener("abort", done); resolve(); };
    const timer = setTimeout(done, ms);
    lifetime.signal.addEventListener("abort", done, { once: true });
    if (lifetime.signal.aborted) done();
  });
}
async function connect(): Promise<void> {
  let retry = 250;
  while (!lifetime.signal.aborted) {
    const tenant = selectedTenant;
    watchController = new AbortController();
    status(value ? "disconnected" : "connecting");
    try {
      // This is the only application read. One query supplies the entire page;
      // the SDK reconstructs its value from snapshot/patch SSE events. Its
      // replica-local policy permits older state, also across reconnections.
      for await (const update of client.watch("pizza.dashboard", { tenant },
        { signal: AbortSignal.any([lifetime.signal, watchController.signal]) })) {
        if (tenant !== selectedTenant) continue;
        remember(update.value);
        value = update.value;
        revision = update.revision;
        received++;
        lastChange = Date.now();
        retry = 250;
        if (!connected) status("connected");
        schedule();
      }
      if (!lifetime.signal.aborted) throw new Error("The watch stream ended");
    } catch (error) {
      if (lifetime.signal.aborted) return;
      if (tenant !== selectedTenant) continue;
      status("disconnected", `Reconnecting in ${(retry / 1000).toFixed(1)}s`);
      element("stale-banner").textContent = "Connection lost. Showing the last received replica state. Reconnecting with a new snapshot…";
      if (!value) text("action-status", `Waiting for the kitchens: ${error instanceof Error ? error.message : "connection unavailable"}`);
    }
    if (tenant !== selectedTenant) continue;
    await wait(retry);
    retry = Math.min(5_000, retry * 2);
  }
}
element<HTMLSelectElement>("tenant").addEventListener("change", () => {
  selectedTenant = element<HTMLSelectElement>("tenant").value;
  history = [];
  status("disconnected", `Loading ${selectedTenant}`);
  text("stale-banner", `Switching tenants. Showing the last received ${value?.tenant ?? "kitchen"} state until the new snapshot arrives.`);
  watchController?.abort(new Error("Tenant changed"));
});
for (const id of ["order", "tip", "pause", "crash"] as const) element(id).addEventListener("click", () => { void action(id); });
for (const button of document.querySelectorAll<HTMLButtonElement>("[data-rate]")) {
  button.addEventListener("click", () => { void action("rate", Number(button.dataset.rate)); });
}
for (const button of document.querySelectorAll<HTMLButtonElement>("[data-filter]")) {
  button.addEventListener("click", () => {
    filter = button.dataset.filter!;
    for (const item of document.querySelectorAll("[data-filter]")) item.setAttribute("aria-pressed", String(item === button));
    renderOrders();
    tick();
  });
}
const chart = element("chart");
chart.addEventListener("pointermove", (event) => { hover = event.clientX - chart.getBoundingClientRect().left; renderChart(); });
chart.addEventListener("pointerleave", () => { hover = null; renderChart(); });
new ResizeObserver(() => renderChart()).observe(chart);
if (paused) {
  showPaused();
  text("action-status", "Automatic arrivals paused. The drones are still delivering.");
}
const localClock = setInterval(tick, 250);
window.addEventListener("pagehide", () => { lifetime.abort(); clearInterval(localClock); }, { once: true });
// A restored page had its stream deliberately closed when it was cached. Reload
// to create one fresh watch and refresh the launcher’s initial pause metadata.
window.addEventListener("pageshow", (event) => { if (event.persisted) window.location.reload(); });
void connect();
void pollStats();
