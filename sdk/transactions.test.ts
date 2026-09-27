import assert from "node:assert/strict";
import { test } from "node:test";
import { define, transaction, type Json } from "./index.ts";

test("transaction plans expose only code-owned aliases and receive arguments without a context", () => {
  const transfer = transaction("internal.transfer", (args: { from: string; to: string; amount: number }) => ({
    calls: [
      { group: "accounts", method: "debit", args: { id: args.from, amount: args.amount } },
      { group: "ledger", method: "credit", args: { id: args.to, amount: args.amount } },
    ], value: { amount: args.amount },
  }));
  const app = define({ http: { transfer }, definitions: [transfer] });
  assert.deepEqual(app.http.transfer, { name: "internal.transfer", kind: "transaction" });
  assert.equal(Object.hasOwn(app.http, "internal.transfer"), false);
  assert.deepEqual(app.definitions[transfer.name].compute({} as never, { from: "a", to: "b", amount: 7 }), {
    calls: [
      { group: "accounts", method: "debit", args: { id: "a", amount: 7 } },
      { group: "ledger", method: "credit", args: { id: "b", amount: 7 } },
    ], value: { amount: 7 },
  });
  assert.ok(Object.isFrozen(transfer));
  assert.ok(Object.isFrozen(app.definitions[transfer.name]));
  assert.deepEqual(Object.keys(define({ definitions: [transfer] }).http), []);
});

test("transactions cannot declare replica-local policy or become maintenance handlers", () => {
  const plan = transaction("plan", (_args: Json) => ({ calls: [] }));
  const forged = { ...plan, consistency: "replica-local", aggregate: { collection: "a", fields: ["id"] } } as never;
  const app = define({ http: { plan: forged } });
  assert.deepEqual(app.http.plan, { name: "plan", kind: "transaction" });
  assert.deepEqual(Object.keys(app.definitions.plan).sort(), ["compute", "kind", "name"]);
  assert.throws(() => define({ tasks: [plan as never] }), /task definitions/);
  assert.throws(() => define({ maintenance: plan } as never), /does not accept "maintenance"/);
  assert.throws(() => transaction("", () => ({ calls: [] })), /nonempty/);
  assert.throws(() => transaction("bad", null as never), /plan function/);
});
