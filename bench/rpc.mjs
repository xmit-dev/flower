import { setTimeout as delay } from "node:timers/promises";
import { createHttp2Transport } from "../sdk/http2.ts";

export class RpcError extends Error {
  /** failure is the method's own { code, message, details? } when Flower reports one. */
  constructor(message, status = 0, code = "NETWORK", failure) {
    super(message);
    this.status = status;
    this.code = code;
    if (failure !== undefined) this.failure = failure;
  }
}

function failureOf(value) {
  if (value === null || typeof value !== "object" || typeof value.code !== "string" || typeof value.message !== "string") return undefined;
  return { code: value.code, message: value.message, ...(value.details === undefined ? {} : { details: value.details }) };
}

// Bound the caller's wait even when another caller owns a shared discovery.
function abortable(promise, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
  });
}

/** One logical invocation keeps exactly the same serialized request on every retry. */
export class BenchmarkClient {
  constructor(cluster, options, signal, phase) {
    this.cluster = cluster;
    this.options = options;
    this.signal = signal;
    this.phase = phase;
    this.sequence = 0;
    this.queryCursor = 0;
    this.queryRoutes = new Map();
    this.replicaCooldowns = new Map();
    this.transport = options.http2 ? createHttp2Transport({ requestTimeoutMs: options.requestTimeoutMs }) : null;
  }

  async close() { await this.transport?.close(); }

  id() { return `bench-${++this.sequence}`; }

  routingSummary() {
    return { mode: this.options.queryRouting ?? "leader", consistency: this.options.readConsistency ?? "fresh", auditConsistency: "fresh",
      nodes: [...this.queryRoutes.values()].map((node) => ({ ...node })) };
  }

  replica(excluded) {
    const members = this.cluster.members.filter((node) => node.process && !node.process.ended && !node.process.intentional);
    const now = performance.now();
    for (let offset = 0; offset < members.length; offset++) {
      const index = (this.queryCursor + offset) % members.length;
      if (excluded.has(members[index]) || (this.replicaCooldowns.get(members[index].url) ?? 0) > now) continue;
      this.queryCursor = (index + 1) % members.length;
      return members[index];
    }
    return null;
  }

  call(name, args, { requestId = this.id(), query = false, replay = false, signal, queryNode } = {}) {
    if (queryNode && !query) throw new TypeError("queryNode is only valid for queries");
    return this.request(name, { name, args, ...(query ? {} : { requestId }) }, {
      path: query ? "/v1/query" : "/v1/call", query, replay, callerSignal: signal, queryNode,
    });
  }

  deploy(bundle) {
    return this.request("deploy", { bundle, requestId: this.id() }, { path: "/admin/deploy", admin: true });
  }

  async request(name, body, { path, admin = false, query = false, replay = false, callerSignal, queryNode }) {
    const phase = this.phase();
    const started = performance.now();
    const outerSignal = callerSignal ? AbortSignal.any([this.signal, callerSignal]) : this.signal;
    const signal = AbortSignal.any([outerSignal, AbortSignal.timeout(this.options.retryBudgetMs)]);
    const serialized = JSON.stringify(body);
    const distributed = query && this.options.queryRouting === "replicas"
      && this.cluster.members?.length > 0;
    // Each failed replica is skipped until every live candidate has had a turn.
    // Cycling an unavailable cluster still backs off rather than spinning.
    const failedReplicas = new Set();
    let attempt = 0;
    let result;
    let succeeded = false;
    let lastError;
    try {
      while (!signal.aborted) {
        let target;
        if (queryNode) target = queryNode;
        else if (distributed) {
          target = this.replica(failedReplicas);
          if (!target) {
            await delay(Math.min(250, Math.max(25, 25 * attempt)), undefined, { signal });
            failedReplicas.clear();
            continue;
          }
        } else if (!this.cluster.leader || this.cluster.leader.process?.ended) {
          try {
            await abortable(this.cluster.discoverLeader({ timeoutMs: this.options.requestTimeoutMs }), signal);
          } catch (error) {
            lastError = error;
            await delay(50, undefined, { signal });
            continue;
          }
        }
        target ??= this.cluster.leader;
        const attemptedLeader = this.cluster.leader;
        const url = target.url ?? this.cluster.url;
        let route;
        if (query) {
          route = this.queryRoutes.get(url);
          if (!route) this.queryRoutes.set(url, route = { id: target.id ?? null, url, attempts: 0, completed: 0, failures: 0 });
        }
        const attemptStarted = performance.now();
        let status = "network";
        let ok = false;
        let duplicate = false;
        try {
          const request = !admin && this.transport ? this.transport.fetch : fetch;
          const response = await request(url + path, {
            method: "POST",
            headers: {
              "content-type": "application/json",
              ...(admin ? { authorization: `Bearer ${this.cluster.adminToken}` } : {}),
            },
            body: serialized,
            signal: AbortSignal.any([signal, AbortSignal.timeout(this.options.requestTimeoutMs)]),
          });
          status = response.status;
          const text = await response.text();
          let data;
          try { data = JSON.parse(text); } catch { data = null; }
          if (!response.ok) throw new RpcError(data?.error?.message ?? text, response.status, data?.error?.code, failureOf(data?.error?.failure));
          if (!data || !Number.isSafeInteger(data.revision) || !Object.hasOwn(data, "value")) {
            throw new RpcError("Malformed successful Flower response", response.status, "INVALID_RESPONSE");
          }
          duplicate = data.duplicate === true;
          result = data;
          succeeded = ok = true;
          return data;
        } catch (error) {
          lastError = error;
          if (error.name === "TimeoutError") status = "timeout";
          if (signal.aborted) status = outerSignal.aborted ? "aborted" : "timeout";
          if (error instanceof RpcError && ![502, 503, 504].includes(error.status)) throw error;
        } finally {
          const measurement = {
            latencyMs: performance.now() - attemptStarted, ok, status,
            retry: attempt++ > 0, duplicate,
          };
          phase.stats.record(name, measurement);
          this.aggregate?.record(name, measurement);
          if (route) { route.attempts++; route[ok ? "completed" : "failures"]++; }
        }
        if (queryNode) {
          await delay(Math.min(250, 25 * attempt), undefined, { signal });
          continue;
        }
        if (distributed) {
          failedReplicas.add(target);
          // Share backoff across callers. A process that has restarted but
          // cannot yet prove freshness should not attract every third new read.
          this.replicaCooldowns.set(url, performance.now() + Math.min(250, 25 * attempt));
          continue;
        }
        // The old address may still answer HTTP while no longer being leader.
        try {
          await abortable(this.cluster.discoverLeader({ timeoutMs: this.options.requestTimeoutMs }), signal);
        } catch (error) { lastError = error; }
        // Once discovery found another serving leader, retry immediately. Keep
        // backoff for overload or uncertain errors against the same leader.
        if (this.cluster.leader === attemptedLeader || !this.cluster.leader) {
          await delay(Math.min(250, 25 * attempt), undefined, { signal });
        }
      }
      throw lastError ?? signal.reason;
    } catch (error) {
      if (outerSignal.aborted) throw outerSignal.reason;
      if (signal.aborted) throw new RpcError(`Retry budget exhausted for ${name}: ${lastError?.message ?? error.message}`, 0, "RETRY_EXHAUSTED");
      throw error;
    } finally {
      phase.ended = Math.max(phase.ended, performance.now());
      const measurement = {
        latencyMs: performance.now() - started, ok: succeeded, duplicate: result?.duplicate === true,
      };
      phase.stats.recordOperation(name + (replay ? ".replay" : ""), measurement);
      this.aggregate?.recordOperation(name + (replay ? ".replay" : ""), measurement);
    }
  }
}
