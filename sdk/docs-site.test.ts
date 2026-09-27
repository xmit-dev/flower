import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import test from "node:test";
// @ts-expect-error The site builder is an untyped Node script.
import { buildDocs } from "../scripts/build-docs.mjs";

test("the website renders and validates in memory without changing its sources", async () => {
  const docs = new URL("../docs/", import.meta.url);
  const snapshot = () => readdirSync(docs, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => [entry.parentPath + "/" + entry.name, readFileSync(entry.parentPath + "/" + entry.name)]);
  const before = snapshot();
  await assert.doesNotReject(() => buildDocs({ check: true }));
  assert.deepEqual(snapshot(), before);
});
