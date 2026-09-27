/** Numeric-only independent arrival accounting shared by both report layouts. */
export function offeredPanel(value) {
  if(value?.mode!=="open-loop")return "";
  const n=value=>Number.isFinite(value)?value.toLocaleString("en-US",{maximumFractionDigits:2}):"—";
  const rows=[["Offered rate / group",`${n(value.ratePerGroup)} / s`],["Scheduled arrivals",n(value.offered)],["Dispatched customer operations",n(value.dispatched)],
    ["Driver capacity drops",n(value.driverDropped)],["Completed arrivals",n(value.completed)],["Failed arrivals",n(value.failed)],
    ["Scheduling lag p99 (dispatched)",`${n(value.schedulingLagMs?.p99)} ms`]];
  return `<section class="panel" id="offered-load"><h2>Independent arrivals, accounted for.</h2><table><thead><tr><th>Open-loop measurement</th><th>Value</th></tr></thead><tbody>${rows.map(([name,value])=>`<tr><th scope="row">${name}</th><td>${value}</td></tr>`).join("")}</tbody></table><p>Arrivals follow a fixed independent clock. The concurrency setting bounds driver slots: a full driver counts drops instead of silently slowing the arrival clock. Primary customer latency starts at the intended arrival, including scheduling delay and retries. HTTP attempt latency still starts at dispatch. Duplicate replay probes are extra traffic; an arrival completes after its optional probe. Driver drops never reach Flower and are separate from server errors and durable mutation goodput. In-flight tails are included in measured duration. This models a bounded load generator, not an infinite upstream queue.</p></section>`;
}
