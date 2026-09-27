import assert from "node:assert/strict";
import test from "node:test";
import { runtimeSettings } from "./runtime-settings.mjs";

test("report budgets without copying credentials or unknown Flower variables", () => {
  assert.deepEqual(runtimeSettings({ FLOWER_EVALUATION_TIMEOUT_MS: "9000",
    FLOWER_RPC_MAX_BYTES: "99999999", FLOWER_PREPARATION_WORKERS: "8", FLOWER_WASM_RECYCLE_BYTES: "1048576", FLOWER_WASM_RECYCLE: "0", FLOWER_WASM_DIRTY_PAGES: "0",
    FLOWER_SNAPSHOT_AFTER_BYTES: "67108864", FLOWER_PEER_TOKEN: "secret",
    FLOWER_KEYRING_FILE: "/private/key-file", FLOWER_TOKEN: "secret", FLOWER_FUTURE_SECRET: "secret" }),
  { FLOWER_PREPARATION_WORKERS: "8", FLOWER_EVALUATION_TIMEOUT_MS: "9000",
    FLOWER_WASM_RECYCLE_BYTES: "1048576", FLOWER_WASM_RECYCLE: "0", FLOWER_WASM_DIRTY_PAGES: "0", FLOWER_RPC_MAX_BYTES: "99999999", FLOWER_SNAPSHOT_AFTER_BYTES: "67108864" });
});

test("report a zero resident-reuse budget explicitly", () => {
  assert.deepEqual(runtimeSettings({ FLOWER_WASM_RECYCLE_BYTES: "0" }),
    { FLOWER_WASM_RECYCLE_BYTES: "0" });
});

test("report deployment page and ordinary writer windows independently", () => {
  assert.deepEqual(runtimeSettings({ FLOWER_DEPLOYMENT_PAGE_MS: "20", FLOWER_WRITER_BATCH_MS: "200" }),
    { FLOWER_WRITER_BATCH_MS: "200", FLOWER_DEPLOYMENT_PAGE_MS: "20" });
  assert.deepEqual(runtimeSettings({ FLOWER_DEPLOYMENT_PAGE_MS: "75" }),
    { FLOWER_DEPLOYMENT_PAGE_MS: "75" });
});

test("report safe OpenTelemetry settings without collector credentials or resource attributes", () => {
  assert.deepEqual(runtimeSettings({ FLOWER_OTEL_ENABLED: "1", OTEL_SDK_DISABLED: "false",
    OTEL_TRACES_SAMPLER: "parentbased_traceidratio", OTEL_TRACES_SAMPLER_ARG: "0.01", OTEL_METRIC_EXPORT_INTERVAL: "1000",
    OTEL_EXPORTER_OTLP_PROTOCOL: "http/json", OTEL_EXPORTER_OTLP_ENDPOINT: "https://secret@collector",
    OTEL_EXPORTER_OTLP_HEADERS: "authorization=secret", OTEL_EXPORTER_OTLP_TRACES_HEADERS: "authorization=secret",
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "https://secret@collector", OTEL_RESOURCE_ATTRIBUTES: "password=secret" }),
  { FLOWER_OTEL_ENABLED: "1", OTEL_SDK_DISABLED: "false", OTEL_TRACES_SAMPLER: "parentbased_traceidratio",
    OTEL_TRACES_SAMPLER_ARG: "0.01", OTEL_EXPORTER_OTLP_PROTOCOL: "http/json", OTEL_METRIC_EXPORT_INTERVAL: "1000" });
});
