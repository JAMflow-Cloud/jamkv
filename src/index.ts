import { type Config, LibSQLKV } from "./core/index.js";

export * from "./core/index.js";

/**
 * Creates and initializes a new KV store.
 * This ensures the schema is ready before returning the instance.
 */
export async function createKV(config: Config): Promise<LibSQLKV> {
  const kv = new LibSQLKV(config);
  await kv.init();
  return kv;
}
