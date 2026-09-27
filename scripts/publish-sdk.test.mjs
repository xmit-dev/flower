import assert from "node:assert/strict";
import test from "node:test";
import { publicationState } from "./publish-sdk.mjs";

const name = "@flower-js/sdk";
const version = "0.1.0";

test("a missing package requires manual bootstrap without attempting publication", async () => {
  assert.equal(await publicationState(name, version, async url => {
    assert.equal(url, "https://registry.npmjs.org/%40flower-js%2Fsdk");
    return new Response("not found", { status: 404 });
  }), "bootstrap");
});

test("existing immutable versions are skipped, unpublished versions proceed", async () => {
  const lookup = async () => Response.json({ name, versions: { "0.1.0": {} } });
  assert.equal(await publicationState(name, version, lookup), "published");
  assert.equal(await publicationState(name, "0.2.0", lookup), "publish");
});

test("registry authentication, rate limits, and outages are not mistaken for bootstrap", async () => {
  for (const status of [401, 403, 429, 500, 503]) {
    await assert.rejects(publicationState(name, version, async () => new Response("failure", { status })),
      new RegExp(`HTTP ${status}`));
  }
  await assert.rejects(publicationState(name, version, async () => { throw new Error("network unavailable"); }), /network unavailable/);
});

test("malformed successful registry responses fail instead of selecting publish", async () => {
  await assert.rejects(publicationState(name, version, async () => new Response("not json")));
  await assert.rejects(publicationState(name, version, async () => Response.json({ name: "another", versions: {} })));
  await assert.rejects(publicationState(name, version, async () => Response.json({ name, versions: [] })));
});
