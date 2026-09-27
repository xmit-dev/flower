import { collection, define, external, mutation, query, v } from "@flower-js/sdk";

const documentId = v.string({ min: 1, max: 256 });
const documents = collection("documents", v.object({ text: v.string({ max: 100_000 }) }));

// Everything that affects the digest is in its input. Workers publish results
// tagged with that input; a changed document makes its old digest stale at once.
const digest = external("digest", {
  input: (ctx, id: string) => {
    const document = ctx.get(documents, id);
    return document && { recipe: "sha256-v1", text: document.text };
  },
  result: v.string({ pattern: /^[0-9a-f]{64}$/ }),
  each: documents,
});

const put = mutation("document.put", { args: v.object({ id: documentId, text: v.string({ max: 100_000 }) }) }, (ctx, input) => {
  ctx.set(documents, input.id, { text: input.text });
  return null;
});

const remove = mutation("document.delete", { args: documentId }, (ctx, id) => {
  ctx.delete(documents, id);
  return null;
});

const get = query("document.get", { args: documentId }, (ctx, id) => {
  const document = ctx.get(documents, id);
  return document && { text: document.text, digest: ctx.get(digest, id) };
});

const app = define({
  uses: [digest],
  http: { "document.put": put, "document.delete": remove, "document.get": get, ...digest.http("digest") },
});
export default app;
