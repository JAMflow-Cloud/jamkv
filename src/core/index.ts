import type {
  Client,
  Config,
  Transaction,
  ResultSet,
  InValue,
  Replicated,
} from "@libsql/client";
import { createClient } from "@libsql/client";
import { secureGenerate } from "unsecure";

// #region Types

export type { Config, Replicated } from "@libsql/client";

type JSONValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JSONValue }
  | JSONValue[];

export type KVValue = JSONValue | Uint8Array<ArrayBuffer>;

export type KVEntry<T = KVValue> = {
  key: string;
  value: T;
  version: string;
};

export type WhereOperator = "=" | ">" | "<" | "LIKE" | "!=";

export type WhereCondition = {
  field: string;
  operator: WhereOperator;
  value: string | number;
};

type LogicResult = {
  op: "AND" | "OR" | "NOT";
  conditions: (WhereCondition | LogicResult)[];
};

export type AndFn = (...args: (WhereCondition | LogicResult)[]) => LogicResult;
export type OrFn = (...args: (WhereCondition | LogicResult)[]) => LogicResult;
export type NotFn = (arg: WhereCondition | LogicResult) => LogicResult;

export type WhereCallback = (args: {
  and: AndFn;
  or: OrFn;
  not: NotFn;
}) => LogicResult;

export type ListOptions = {
  prefix?: string;
  limit?: number;
  cursor?: string;
  // JSON filtering
  where?: WhereCondition | WhereCallback;
  reverse?: boolean;
};

export type SetOptions = {
  expireIn?: number; // milliseconds
};

// #region Core Class

/**
 * The core logic shared between the main Client and Transactions.
 * This allows transactions to function exactly like the main KV store.
 */
export abstract class CoreKV {
  protected tableName: string = "kv_store";

  // Abstract method to be implemented by ClientKV and TransactionKV
  protected abstract execute(sql: string, args: InValue[]): Promise<ResultSet>;

  async get<T = KVValue>(key: string): Promise<KVEntry<T> | null> {
    const rs = await this.execute(
      `SELECT * FROM ${this.tableName} WHERE key = ?`,
      [key],
    );

    if (rs.rows.length === 0 || rs.rows[0] === undefined) return null;
    const row = rs.rows[0];

    // Lazy Expiration Check
    if (row.expires_at && (row.expires_at as number) < Date.now()) {
      // Optimization: We try to delete it, but we don't await it to keep 'get' fast.
      // Note: In a transaction context, we do overwrite this method to await the deletion.
      this.del(key).catch(() => {});
      return null;
    }

    return {
      key: row.key as string,
      value: decodeValue(row) as T,
      version: row.version as string,
    };
  }

  async getMany<T = KVValue>(keys: string[]): Promise<(KVEntry<T> | null)[]> {
    if (keys.length === 0) return [];

    // Generate placeholders (?,?,?)
    const placeholders = keys.map(() => "?").join(",");
    const rs = await this.execute(
      `SELECT * FROM ${this.tableName} WHERE key IN (${placeholders})`,
      keys,
    );

    const map = new Map<string, any>();
    const now = Date.now();

    for (const row of rs.rows) {
      if (row.expires_at && (row.expires_at as number) < now) {
        // Similar lazy expiration handling as in get()
        this.del(row.key as string).catch(() => {});
        continue;
      }
      map.set(row.key as string, row);
    }

    return keys.map((key) => {
      const row = map.get(key);
      if (!row) return null;
      return {
        key: row.key as string,
        value: decodeValue(row) as T,
        version: row.version as string,
      };
    });
  }

  async set(key: string, value: KVValue, options?: SetOptions): Promise<void> {
    const { blob, text, type } = encodeValue(value);
    const version = generateVersion();
    const expiresAt = options?.expireIn ? Date.now() + options.expireIn : null;

    await this.execute(
      `INSERT INTO ${this.tableName} (key, value_blob, value_text, value_type, version, expires_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
  value_blob = excluded.value_blob,
  value_text = excluded.value_text,
  value_type = excluded.value_type,
  version = excluded.version,
  expires_at = excluded.expires_at
`,
      [key, blob, text, type, version, expiresAt],
    );
  }

  async del(key: string): Promise<void> {
    await this.execute(`DELETE FROM ${this.tableName} WHERE key = ?`, [key]);
  }

  async cleanupExpired(): Promise<void> {
    await this.execute(
      `DELETE FROM ${this.tableName} WHERE expires_at IS NOT NULL AND expires_at < ?`,
      [Date.now()],
    );
  }

  async list<T = KVValue>(options: ListOptions = {}): Promise<KVEntry<T>[]> {
    let sql = `SELECT * FROM ${this.tableName} WHERE 1=1`;
    const args: InValue[] = [];

    // 1. Filter expired
    sql += ` AND (expires_at IS NULL OR expires_at > ?)`;
    args.push(Date.now());

    // 2. Prefix
    if (options.prefix) {
      sql += ` AND key LIKE ?`;
      args.push(`${options.prefix}%`);
    }

    // 3. JSON Filtering
    if (options.where) {
      if (typeof options.where === "function") {
        const and: AndFn = (...args) => ({ op: "AND", conditions: args });
        const or: OrFn = (...args) => ({ op: "OR", conditions: args });
        const not: NotFn = (arg) => ({ op: "NOT", conditions: [arg] });

        const logicTree = options.where({ and, or, not });
        const { sql: whereSql, args: whereArgs } =
          this.buildWhereSql(logicTree);

        // We wrap in ( ... ) to ensure precedence
        sql += ` AND value_type = 'json' AND (${whereSql})`;
        args.push(...whereArgs);
      } else {
        // Simple object case
        const safeOps = ["=", ">", "<", "LIKE", "!="];
        const op = safeOps.includes(options.where.operator)
          ? options.where.operator
          : "=";

        sql += ` AND value_type = 'json' AND json_extract(value_text, ?) ${op} ?`;
        args.push(`$.${options.where.field}`, options.where.value);
      }
    }

    // 4. Ordering
    sql += ` ORDER BY key ${options.reverse ? "DESC" : "ASC"}`;

    // 5. Pagination
    if (options.limit) {
      sql += ` LIMIT ?`;
      args.push(options.limit);
    }

    const rs = await this.execute(sql, args);

    return rs.rows.map((row) => ({
      key: row.key as string,
      value: decodeValue(row) as T,
      version: row.version as string,
    }));
  }

  private buildWhereSql(condition: WhereCondition | LogicResult): {
    sql: string;
    args: InValue[];
  } {
    if ("op" in condition) {
      // LogicResult
      if (condition.op === "NOT") {
        const first = condition.conditions[0];
        if (!first) throw new Error("NOT operator requires a condition");
        const inner = this.buildWhereSql(first);
        return {
          sql: `NOT (${inner.sql})`,
          args: inner.args,
        };
      }

      const parts: string[] = [];
      const args: InValue[] = [];

      for (const cond of condition.conditions) {
        const res = this.buildWhereSql(cond);
        parts.push(res.sql);
        args.push(...res.args);
      }

      return {
        sql: `(${parts.join(` ${condition.op} `)})`,
        args,
      };
    } else {
      // WhereCondition
      const safeOps = ["=", ">", "<", "LIKE", "!="];
      const op = safeOps.includes(condition.operator)
        ? condition.operator
        : "=";

      return {
        sql: `json_extract(value_text, ?) ${op} ?`,
        args: [`$.${condition.field}`, condition.value],
      };
    }
  }
}

// #region Implementation Classes

/**
 * Transaction Wrapper.
 * Extends CoreKV so it has all the get/set/list methods.
 * Adds transaction-specific controls.
 */
export class KVTransaction extends CoreKV {
  constructor(
    private tx: Transaction,
    tableName: string,
  ) {
    super();
    this.tableName = tableName;
  }

  protected async execute(sql: string, args: InValue[]): Promise<ResultSet> {
    return this.tx.execute({ sql, args });
  }

  override async get<T = KVValue>(key: string): Promise<KVEntry<T> | null> {
    const rs = await this.execute(
      `SELECT * FROM ${this.tableName} WHERE key = ?`,
      [key],
    );

    if (rs.rows.length === 0 || rs.rows[0] === undefined) return null;
    const row = rs.rows[0];

    if (row.expires_at && (row.expires_at as number) < Date.now()) {
      await this.del(key);
      return null;
    }

    return {
      key: row.key as string,
      value: decodeValue(row) as T,
      version: row.version as string,
    };
  }

  override async getMany<T = KVValue>(
    keys: string[],
  ): Promise<(KVEntry<T> | null)[]> {
    if (keys.length === 0) return [];

    // Generate placeholders (?,?,?)
    const placeholders = keys.map(() => "?").join(",");
    const rs = await this.execute(
      `SELECT * FROM ${this.tableName} WHERE key IN (${placeholders})`,
      keys,
    );

    const map = new Map<string, any>();
    const now = Date.now();

    for (const row of rs.rows) {
      if (row.expires_at && (row.expires_at as number) < now) {
        await this.del(row.key as string);
        continue;
      }
      map.set(row.key as string, row);
    }

    return keys.map((key) => {
      const row = map.get(key);
      if (!row) return null;
      return {
        key: row.key as string,
        value: decodeValue(row) as T,
        version: row.version as string,
      };
    });
  }

  /**
   * Commits the transaction.
   */
  async commit(): Promise<void> {
    await this.tx.commit();
  }

  /**
   * Rolls back the transaction.
   */
  async rollback(): Promise<void> {
    await this.tx.rollback();
  }

  /**
   * Closes the transaction.
   */
  async close(): Promise<void> {
    this.tx.close();
  }
}

/**
 * Main KV Client.
 */
export class LibSQLKV extends CoreKV {
  private client: Client;

  constructor(config: Config) {
    super();
    this.client = createClient(config);
  }

  /**
   * Internal Init. Used by createKV factory.
   */
  async init() {
    await this.client.execute(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        key TEXT PRIMARY KEY,
        value_blob BLOB,
        value_text TEXT,
        value_type TEXT CHECK(value_type IN ('json', 'string', 'number', 'binary', 'boolean')),
        version TEXT NOT NULL,
        expires_at INTEGER,
        created_at INTEGER DEFAULT (unixepoch('now') * 1000)
      )
    `);
    await this.client.execute(
      `CREATE INDEX IF NOT EXISTS idx_kv_expires ON ${this.tableName}(expires_at)`,
    );
  }

  protected async execute(sql: string, args: InValue[]): Promise<ResultSet> {
    return this.client.execute({ sql, args });
  }

  /**
   * Starts a new transaction.
   * @param mode 'write' | 'read' | 'deferred' (defaults to write for KV ops)
   */
  async transaction(
    mode: "write" | "read" | "deferred" = "write",
  ): Promise<KVTransaction> {
    const tx = await this.client.transaction(mode);
    return new KVTransaction(tx, this.tableName);
  }

  async sync(): Promise<Replicated> {
    return this.client.sync();
  }

  /**
   * Closes the underlying client connection.
   */
  close() {
    this.client.close();
  }
}

// #region Internals

function generateVersion(): string {
  return secureGenerate({
    length: 16,
    numbers: true,
    lowercase: true,
    timestamp: true,
    uppercase: false,
    specials: false,
  });
}

function encodeValue(value: KVValue): {
  blob: ArrayBuffer | null;
  text: string | null;
  type: string;
} {
  if (value instanceof Uint8Array) {
    return { blob: toArrayBuffer(value), text: null, type: "binary" };
  }
  if (typeof value === "string") {
    return { blob: null, text: value, type: "string" };
  }
  if (typeof value === "number") {
    return { blob: null, text: String(value), type: "number" };
  }
  if (typeof value === "boolean") {
    return { blob: null, text: String(value), type: "boolean" };
  }
  // Default to JSON
  return { blob: null, text: JSON.stringify(value), type: "json" };
}

function decodeValue(row: any): KVValue {
  if (
    !("value_type" in row) &&
    !("value_blob" in row) &&
    !("value_text" in row)
  ) {
    throw new Error("Invalid row format for decoding value.");
  }

  if (row.value_type === "binary") {
    // libsql returns ArrayBuffer, convert back to Uint8Array
    return new Uint8Array(row.value_blob);
  }
  if (row.value_type === "number") return Number(row.value_text);
  if (row.value_type === "boolean") return row.value_text === "true";
  if (row.value_type === "json") return JSON.parse(row.value_text);
  return row.value_text; // string
}

function toArrayBuffer(arr: Uint8Array<ArrayBuffer>): ArrayBuffer {
  if (arr.byteLength === arr.buffer.byteLength && arr.byteOffset === 0) {
    return arr.buffer;
  }
  return arr.buffer.slice(arr.byteOffset, arr.byteOffset + arr.byteLength);
}
