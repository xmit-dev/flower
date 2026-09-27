import { run } from "./goblin-pizza.mjs";
const options = JSON.parse(process.argv[2]);
let parentLostAlready = false;
const parentLost = () => {
  if (parentLostAlready) return;
  parentLostAlready = true;
  process.kill(process.pid, "SIGTERM");
};
const send = (message) => {
  if (!process.connected) return;
  // An IPC race must trigger normal local cleanup, never prevent it through an
  // unhandled callback error. The parent also tracks each live server PID.
  try { process.send(message, (error) => { if (error) parentLost(); }); }
  catch { parentLost(); }
};
options.onProcess = (event) => send({ type: "server-process", event });
process.once("disconnect", parentLost);
let observer;
try {
  if (options.otelCapture) {
    const { otelResourceAttributes } = await import("./otel-capture.mjs");
    process.env.OTEL_RESOURCE_ATTRIBUTES = otelResourceAttributes(options.otelCapture.runId, options.otelCapture.group);
  }
  if (options.mixedProfile) {
    const { installMixedObserver } = await import("./profile-mixed-observer.mjs");
    observer = installMixedObserver(options.mixedProfile);
  }
  const message = (type) => new Promise((resolve) => {
    const receive = (value) => {
      if (value?.type !== type) return;
      process.off("message", receive);
      resolve(value);
    };
    process.on("message", receive);
  });
  // With shared hosts the coordinator kills one; this group observes it.
  const hostCrash = options.attach ? async (cluster) => {
    const crash = await message("host-crash");
    const observation = await cluster.observeHostCrash(crash.host, crash.crashedAt);
    const restarted = message("host-restarted");
    send({ type: "host-recovered" });
    await restarted;
    return cluster.observeHostRestart(crash.host, observation);
  } : undefined;
  const report = await run(options, { hostCrash, ready: (signal) => new Promise((resolve, reject) => {
    const cleanup = () => { process.off("message", start); process.off("disconnect", disconnected); signal.removeEventListener("abort", abort); };
    const abort = () => { cleanup(); reject(signal.reason); };
    const disconnected = () => { cleanup(); reject(new Error("Coordinator disconnected before load started")); };
    const start = (message) => {
      if (message?.type !== "start") return;
      observer?.begin(message.startAt);
      cleanup(); resolve(message.startAt);
    };
    process.on("message", start);
    process.once("disconnect", disconnected);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort(); else send({ type: "ready" });
  }) });
  if (!report.passed) process.exitCode = 1;
  await observer?.finish(report.runId);
} catch (error) { console.error(error); process.exitCode = 1; }
finally { observer?.restore(); process.off("disconnect", parentLost); if (process.connected) process.disconnect(); }
