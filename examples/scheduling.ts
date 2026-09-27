import { collection, define, mutation, query, v } from "../sdk/index.ts";
import { scheduler } from "../sdk/scheduler.ts";

interface Document {
  text: string;
  status: "draft" | "published";
  version: number;
  updatedAt: number;
  publishedAt: number | null;
}

const id = v.string({ min: 1 });
export const documents = collection<Document>("scheduledDocuments");

// Ordinary private TypeScript business logic runs later in a Raft transaction.
export const publishDocument = mutation("internal.documents.publish", {
  args: v.object({ id, version: v.int({ min: 1 }) }),
}, (ctx, args) => {
  const document = ctx.get(documents, args.id);
  if (document === null || document.version !== args.version) return null;
  ctx.set(documents, args.id, { ...document, status: "published", publishedAt: ctx.now() });
  return null;
});

export const timers = scheduler("publicationTimers", { publish: publishDocument }, {
  maxAttempts: 3, retryDelayMs: 1_000, maxRetryDelayMs: 10_000,
});

export const updateDocument = mutation("internal.documents.update", {
  args: v.object({ id, text: v.string(), publishAfterMs: v.int({ min: 0 }) }),
}, (ctx, args) => {
  const version = (ctx.get(documents, args.id)?.version ?? 0) + 1;
  const document: Document = { text: args.text, status: "draft", version, updatedAt: ctx.now(), publishedAt: null };
  ctx.set(documents, args.id, document);
  // Reusing this ID replaces the previous timer, so each edit restarts the delay.
  const timer = timers.after(ctx, `publish:${args.id}`, args.publishAfterMs, "publish", { id: args.id, version });
  return { document, timer };
});

export const getDocument = query("internal.documents.get", { args: id }, (ctx, key) => ctx.get(documents, key));
export const cancelPublication = mutation("internal.documents.cancelPublication", { args: id }, (ctx, key) => timers.cancel(ctx, `publish:${key}`));
export const publicationStatus = query("internal.documents.publication", { args: id }, (ctx, key) => timers.get(ctx, `publish:${key}`));
export const retryPublication = mutation("internal.documents.retryPublication", {
  args: v.object({ id, delayMs: v.optional(v.int({ min: 0 })) }),
}, (ctx, args) => timers.retry(ctx, `publish:${args.id}`, args.delayMs));

const app = define({
  uses: [timers],
  http: {
    "documents.update": updateDocument,
    "documents.get": getDocument,
    "documents.cancelPublication": cancelPublication,
    "documents.publication": publicationStatus,
    "documents.retryPublication": retryPublication,
  },
});
export default app;
