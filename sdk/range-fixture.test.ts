// Independent in-memory range evaluator for temporal helper policy tests.
import type { IndexScalar, RangePage, RangeQuery } from "./index.ts";
function compare(a: IndexScalar, b: IndexScalar): number {
  const type = (v: IndexScalar) => v === null ? 0 : typeof v === "boolean" ? 1 : typeof v === "number" ? 2 : 3;
  if (type(a) !== type(b)) return type(a) - type(b);
  return a === b ? 0 : a! < b! ? -1 : 1;
}
export function memoryRange<T>(query: RangeQuery<T>, input: { key: string; value: T }[]): RangePage<T> {
  const options = query.options, prefix = options.prefix ?? [];
  const rows = input.filter((row) => {
    const value = row.value as Record<string, IndexScalar>;
    if (!value || typeof value !== "object") return false;
    if (query.fields.some((field) => !Object.hasOwn(value, field) || (value[field] !== null && !["number","boolean","string"].includes(typeof value[field])))) return false;
    if (prefix.some((part, index) => compare(part, value[query.fields[index]]) !== 0)) return false;
    const next = value[query.fields[prefix.length]];
    return (!Object.hasOwn(options,"gt") || compare(next,options.gt!)>0) &&
      (!Object.hasOwn(options,"gte") || compare(next,options.gte!)>=0) &&
      (!Object.hasOwn(options,"lt") || compare(next,options.lt!)<0) &&
      (!Object.hasOwn(options,"lte") || compare(next,options.lte!)<=0);
  }).sort((a,b) => {
    for (const field of query.fields) { const order = compare((a.value as any)[field],(b.value as any)[field]); if(order) return order; }
    return a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
  });
  if(options.reverse) rows.reverse();
  // Cursor carries tuple and key because helpers can delete each previous page.
  const position = (row: typeof rows[number]) => [...query.fields.map((field) => (row.value as any)[field]),row.key] as IndexScalar[];
  const after = options.after === undefined ? null : JSON.parse(options.after) as IndexScalar[];
  const remaining = after === null ? rows : rows.filter((row) => {
    const tuple = position(row);
    for(let i=0;i<tuple.length;i++) { const order = compare(tuple[i],after[i]); if(order) return options.reverse ? order<0 : order>0; }
    return false;
  });
  const page = remaining.slice(0,options.limit);
  return {rows:page,cursor:remaining.length > page.length ? JSON.stringify(position(page.at(-1)!)) : null};
}
