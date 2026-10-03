import { collectionInfo, fail, plainObject, requireName, type Collection, type IndexMap, type Principal, type Row } from "./core.ts";
import { canonicalJson, type Json } from "./json.ts";

/** Canonical transport: integers never pass through JavaScript floating point. */
export type SqlCell = null | { readonly integer: string } | { readonly real: number } | { readonly text: string };
export type JsonScalar = null | boolean | number | string;
export interface SqlColumn {
  readonly name: string;
  readonly type: "integer" | "real" | "text";
  readonly nullable?: boolean;
  readonly description?: string;
  readonly logicalType?: string;
  readonly unit?: string;
  /** Native projection, after collection field redaction. Missing values become SQL NULL. */
  readonly sourceField?: string;
  /** Native projection of the encoded source key. Mutually exclusive with sourceField. */
  readonly sourceKey?: true;
}
export interface SqlIndex { readonly name: string; readonly columns: readonly string[] }
export interface SqlRelationship {
  readonly name: string;
  readonly fromTable: string;
  readonly fromColumns: readonly string[];
  readonly toTable: string;
  readonly toColumns: readonly string[];
  readonly description?: string;
}
export interface SqlReadContext {
  now(): number;
  clock(): number;
  changesAt(time: number | null): void;
  principal(): Principal | null;
  history(): { readonly database: string; readonly incarnation: string } | null;
  get<T, K extends Json>(collection: Collection<T, K, any>, key: K): T | null;
  scan<T, K extends Json, I extends IndexMap>(collection: Collection<T, K, I>, options?: import("./core.ts").ScanOptions<I>): Row<T, K>[];
  range<T, K>(range: import("./core.ts").RangeQuery<T, K>): import("./core.ts").RangePage<T, K>;
  query<T>(query: import("./core.ts").Query<T, any>): T[];
}
export type SqlProviderContext = SqlReadContext;
export type SqlAuthorityContext = SqlReadContext;
export interface SqlProviderInput {
  readonly source: { readonly name: string };
  /** A bounded native batch, already row-filtered and field-redacted for the effective invoker. */
  readonly rows: readonly Row<any, Json>[];
  /** Only the context returned by the deployed authority callback, never the raw caller purpose. */
  readonly purpose: Json;
}
export interface SqlProjectedRow {
  /** Batch-local source ordinal. One input can expand to several relational rows. */
  readonly sourceRow: number;
  readonly cells: readonly SqlCell[];
}
export interface SqlProviderBatch {
  readonly rows: readonly SqlProjectedRow[];
  /** Trusted internal disclosure evidence. Never include this in the public SqlResult. */
  readonly provenance?: readonly Json[];
}
export interface SqlAuthorityRequest {
  readonly catalog: string;
  readonly invoker: Principal | null;
  readonly purpose: Json;
}
export interface SqlAuthorityDecision {
  readonly principal: Principal;
  readonly context: Json;
  /** Explicitly permitted logical table names. Discovery is filtered by this allowlist too. */
  readonly tables: readonly string[];
}
export interface SqlTableSpec {
  readonly name: string;
  readonly source?: Collection<any, any, any>;
  readonly sources?: readonly Collection<any, any, any>[];
  readonly columns: readonly SqlColumn[];
  readonly indexes?: readonly SqlIndex[];
  readonly description?: string;
  readonly grain?: string;
  readonly coverage?: string;
  readonly project: (ctx: SqlProviderContext, input: SqlProviderInput) => SqlProviderBatch;
}
export interface SqlTable extends SqlTableSpec {
  readonly kind: "sqlTable";
  readonly sources: readonly Collection<any, any, any>[];
}
export interface SqlCatalogSpec {
  readonly name: string;
  readonly version: string;
  readonly tables: readonly SqlTable[];
  readonly relationships?: readonly SqlRelationship[];
  readonly authorize: (ctx: SqlAuthorityContext, request: SqlAuthorityRequest) => SqlAuthorityDecision | null;
}
export interface SqlCatalog extends SqlCatalogSpec { readonly kind: "sqlCatalog" }
export interface SqlQueryInput {
  readonly sql: string;
  readonly parameters?: readonly JsonScalar[];
  readonly maxRows?: number;
  /** Ephemeral query operation name, not a row identifier or authorization capability. */
  readonly queryId?: string;
}
export interface SqlResultColumn { readonly name: string; readonly logicalType?: string; readonly unit?: string }
export interface SqlResult {
  readonly columns: readonly SqlResultColumn[];
  readonly rows: readonly (readonly SqlCell[])[];
  readonly resultTruncated: boolean;
  readonly warnings: readonly string[];
  readonly catalogVersion: string;
}
export interface SqlExecution { readonly result: SqlResult; readonly provenance: readonly Json[] }
export type SqlCatalogColumn = Omit<SqlColumn, "sourceField" | "sourceKey">;
export interface SqlCatalogInfo {
  readonly name: string;
  readonly catalogVersion: string;
  readonly tables: readonly { readonly name: string; readonly columns: readonly SqlCatalogColumn[]; readonly description?: string; readonly grain?: string; readonly coverage?: string }[];
  readonly relationships: readonly SqlRelationship[];
}
export interface SqlExplain {
  readonly catalogVersion: string;
  readonly plan: readonly { readonly detail: string }[];
  readonly warnings: readonly string[];
}
export interface SqlContext {
  query(catalog: SqlCatalog, input: SqlQueryInput, purpose?: Json): SqlExecution;
  catalog(catalog: SqlCatalog, purpose?: Json): SqlCatalogInfo;
  explain(catalog: SqlCatalog, input: SqlQueryInput, purpose?: Json): SqlExplain;
  cancel(catalog: SqlCatalog, queryId: string, purpose?: Json): boolean;
}

const tables = new WeakSet<object>();
const catalogs = new WeakSet<object>();
const reserved = new Set(["sql_tables", "sql_columns", "sql_relationships", "sqlite_master", "sqlite_schema", "sqlite_temp_master", "sqlite_temp_schema"]);
function identifier(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || !/^[a-z][a-z0-9_]{0,62}$/.test(value) || reserved.has(value)) throw new TypeError(`${label} must be an unreserved lowercase SQL identifier`);
}
function metadata(value: unknown, label: string): void {
  if (value !== undefined && (typeof value !== "string" || value.length > 4096)) throw new TypeError(`${label} must be at most 4096 characters`);
}
function distinct<T>(items: readonly T[], key: (item: T) => string, label: string): void {
  if (new Set(items.map(key)).size !== items.length) throw new TypeError(`${label} must be distinct`);
}
/** Explicit, curated relation. Declaring a collection never exposes it to SQL. */
export function sqlTable(spec: SqlTableSpec): SqlTable {
  const input = plainObject(spec, "SQL table", ["name", "source", "sources", "columns", "indexes", "description", "grain", "coverage", "project"]);
  identifier(input.name, "SQL table name");
  if ((input.source === undefined) === (input.sources === undefined)) throw new TypeError("SQL table requires exactly one of source or sources");
  const sources = input.source === undefined ? input.sources : [input.source];
  if (!Array.isArray(sources) || sources.length < 1 || sources.length > 8 || sources.some((source) => !collectionInfo(source))) throw new TypeError("SQL sources must contain 1 to 8 declared collections");
  distinct(sources, (source) => source.name, "SQL sources");
  if (!Array.isArray(input.columns) || !input.columns.length || input.columns.length > 64) throw new TypeError("SQL table requires 1 to 64 columns");
  const columns = input.columns.map((raw) => {
    const column = plainObject(raw, "SQL column", ["name", "type", "nullable", "description", "logicalType", "unit", "sourceField", "sourceKey"]);
    identifier(column.name, "SQL column name");
    if (!["integer", "real", "text"].includes(column.type as string)) throw new TypeError("SQL column type is integer, real or text");
    if (column.nullable !== undefined && typeof column.nullable !== "boolean") throw new TypeError("SQL nullable must be boolean");
    if (column.sourceField !== undefined) requireName(column.sourceField, "SQL source field");
    if (column.sourceKey !== undefined && column.sourceKey !== true) throw new TypeError("SQL sourceKey must be true");
    if (column.sourceField !== undefined && column.sourceKey) throw new TypeError("SQL column cannot declare sourceField and sourceKey together");
    if (column.sourceKey && column.type !== "text") throw new TypeError("SQL source keys are encoded text");
    for (const key of ["description", "logicalType", "unit"] as const) metadata(column[key], `SQL column ${key}`);
    return Object.freeze({ ...column }) as unknown as SqlColumn;
  });
  distinct(columns, (column) => column.name, "SQL column names");
  const indexes = input.indexes ?? [];
  if (!Array.isArray(indexes) || indexes.length > 16) throw new TypeError("SQL indexes must be an array of at most 16 entries");
  const copiedIndexes = indexes.map((raw) => {
    const index = plainObject(raw, "SQL index", ["name", "columns"]);
    requireName(index.name, "SQL index name");
    if (!Array.isArray(index.columns) || !index.columns.length || index.columns.some((name) => typeof name !== "string")) throw new TypeError("SQL index requires column names");
    distinct(index.columns, (name) => name, "SQL index columns");
    const fields = index.columns.map((name) => columns.find((column) => column.name === name)?.sourceField);
    if (fields.some((field) => field === undefined) || sources.some((source) => canonicalJson(source.indexes[index.name as string] ?? []) !== canonicalJson(fields as Json))) throw new TypeError("SQL indexes require matching native sourceField indexes on every source");
    return Object.freeze({ name: index.name as string, columns: Object.freeze([...index.columns] as string[]) });
  });
  distinct(copiedIndexes, (index) => index.name, "SQL index names");
  if (typeof input.project !== "function") throw new TypeError("SQL table requires a pure projector");
  for (const key of ["description", "grain", "coverage"] as const) metadata(input[key], `SQL table ${key}`);
  const table = Object.freeze({ ...spec, kind: "sqlTable" as const, sources: Object.freeze([...sources]), columns: Object.freeze(columns), indexes: Object.freeze(copiedIndexes) });
  tables.add(table);
  return table;
}
/** Register only in define({sql:[catalog]}). Authority is required, including for metadata discovery. */
export function sqlCatalog(spec: SqlCatalogSpec): SqlCatalog {
  const input = plainObject(spec, "SQL catalog", ["name", "version", "tables", "relationships", "authorize"]);
  requireName(input.name, "SQL catalog name");
  if ((input.name as string).length > 128) throw new TypeError("SQL catalog name exceeds 128 characters");
  requireName(input.version, "SQL catalog version");
  if ((input.version as string).length > 128) throw new TypeError("SQL catalog version exceeds 128 characters");
  if (!Array.isArray(input.tables) || !input.tables.length || input.tables.length > 64 || input.tables.some((table) => !tables.has(table))) throw new TypeError("SQL catalog requires 1 to 64 sqlTable declarations");
  distinct(input.tables, (table) => table.name, "SQL table names");
  if (typeof input.authorize !== "function") throw new TypeError("SQL catalog requires a pure authority callback");
  const relationships = input.relationships ?? [];
  if (!Array.isArray(relationships) || relationships.length > 128) throw new TypeError("SQL relationships must be an array of at most 128 entries");
  const copied = relationships.map((raw) => {
    const entry = plainObject(raw, "SQL relationship", ["name", "fromTable", "fromColumns", "toTable", "toColumns", "description"]);
    identifier(entry.name, "SQL relationship name");
    for (const prefix of ["from", "to"] as const) {
      const table = (input.tables as SqlTable[]).find((table) => table.name === entry[`${prefix}Table`]);
      const columns = entry[`${prefix}Columns`];
      if (!table || !Array.isArray(columns) || !columns.length || columns.some((name) => !table.columns.some((column) => column.name === name))) throw new TypeError("SQL relationship refers to declared tables and columns");
      distinct(columns, (name) => name, "SQL relationship columns");
    }
    if ((entry.fromColumns as string[]).length !== (entry.toColumns as string[]).length) throw new TypeError("SQL relationship column counts differ");
    metadata(entry.description, "SQL relationship description");
    return Object.freeze({ ...entry, fromColumns: Object.freeze([...(entry.fromColumns as string[])]), toColumns: Object.freeze([...(entry.toColumns as string[])]) }) as unknown as SqlRelationship;
  });
  distinct(copied, (relationship) => relationship.name, "SQL relationship names");
  const catalog = Object.freeze({ ...spec, kind: "sqlCatalog" as const, tables: Object.freeze([...input.tables] as SqlTable[]), relationships: Object.freeze(copied) });
  catalogs.add(catalog);
  return catalog;
}

/** SDK internals, not exported by the package entry point. */
export function sqlDeclaration(catalog: SqlCatalog) {
  if (!catalogs.has(catalog)) throw new TypeError("SQL registration requires sqlCatalog declarations");
  const authority = `${catalog.name}.$sql.authority`;
  return {
    manifest: {
      name: catalog.name, version: catalog.version, authority,
      tables: catalog.tables.map((table) => ({
        name: table.name, sources: table.sources.map((source) => ({ name: source.name, jsonKey: Boolean(collectionInfo(source)?.key) })),
        columns: table.columns, indexes: table.indexes ?? [], provider: `${catalog.name}.$sql.provider.${table.name}`,
        ...(table.description === undefined ? {} : { description: table.description }),
        ...(table.grain === undefined ? {} : { grain: table.grain }),
        ...(table.coverage === undefined ? {} : { coverage: table.coverage }),
      })), relationships: catalog.relationships ?? [],
    },
    definitions: [
      { kind: "sqlAuthority" as const, name: authority, compute: catalog.authorize },
      ...catalog.tables.map((table) => ({ kind: "sqlProvider" as const, name: `${catalog.name}.$sql.provider.${table.name}`,
        compute(ctx: SqlProviderContext, input: SqlProviderInput): SqlProviderBatch {
          const source = table.sources.find((source) => source.name === input.source.name);
          if (!source) fail("SQL_PROVIDER_INVALID", "Unregistered SQL source");
          const decoded = collectionInfo(source)?.key ? { ...input, rows: input.rows.map((row) => ({ ...row, key: JSON.parse(row.key as string) as Json })) } : input;
          return table.project(ctx, decoded);
        } })),
    ],
  };
}
/** SDK internals: one native host operation, no client-supplied principal. */
export function bindSql(host: Record<string, (...args: any[]) => any>): SqlContext {
  function call(operation: string, catalog: SqlCatalog, input: Json, purpose: Json = null): any {
    if (!catalogs.has(catalog)) throw new TypeError("SQL operation requires a sqlCatalog declaration");
    if (typeof host.sql !== "function") fail("SQL_UNAVAILABLE", "This Flower runtime does not support native SQL");
    return host.sql(operation, catalog.name, input, purpose);
  }
  return Object.freeze({
    query: (catalog: SqlCatalog, input: SqlQueryInput, purpose?: Json) => call("query", catalog, input as unknown as Json, purpose),
    catalog: (catalog: SqlCatalog, purpose?: Json) => call("catalog", catalog, null, purpose),
    explain: (catalog: SqlCatalog, input: SqlQueryInput, purpose?: Json) => call("explain", catalog, input as unknown as Json, purpose),
    cancel: (catalog: SqlCatalog, queryId: string, purpose?: Json) => call("cancel", catalog, { queryId }, purpose),
  });
}
