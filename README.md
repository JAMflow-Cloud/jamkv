# jamkv

A lightweight, LibSQL-based Key-Value store for Node.js and Deno.

> [!NOTE]
> Currently, `@libsql/client-wasm` is not supported, so browser usage is out of scope for now.

## Installation

```bash
pnpm add jamkv
```

## Usage

### Initialization

```typescript
import { createKV } from "jamkv";

const kv = await createKV({
  url: "libsql://your-database.turso.io",
  authToken: "your-auth-token",
});
```

### Basic Operations

```typescript
// Set a value
await kv.set("user:1", { name: "Alice", age: 30 });

// Get a value
const user = await kv.get("user:1");
console.log(user?.value);

// Delete a value
await kv.del("user:1");
```

### Expiration

```typescript
// Set a value that expires in 5 seconds
await kv.set("temp-key", "temp-value", { expireIn: 5000 });
```

### Listing

```typescript
// List all keys
const all = await kv.list();

// List with prefix
const users = await kv.list({ prefix: "user:" });

// Filter by JSON field
const adults = await kv.list({
  where: {
    field: "age",
    operator: ">",
    value: 18,
  },
});
```

> [!NOTE]
> The `where` option currently only supports filtering a single top-level JSON field for entries where the value is a JSON object.

### Transactions

```typescript
const tx = await kv.transaction();

try {
  await tx.set("key1", "value1");
  await tx.set("key2", "value2");

  // Read your writes within the transaction
  const val = await tx.get("key1");

  await tx.commit();
} catch (error_) {
  await tx.rollback();
  console.error("Transaction failed:", error_);
}
```

## API

### `createKV(config: Config): Promise<LibSQLKV>`

Creates and initializes the KV store. `Config` is the standard `@libsql/client` configuration object.

### `LibSQLKV`

#### `set<T>(key: string, value: KVValue, options?: SetOptions): Promise<void>`

Sets a value for a key.

- `value`: Can be string, number, boolean, JSON object/array, or Uint8Array.
- `options.expireIn`: Time in milliseconds until the key expires.

#### `get<T>(key: string): Promise<KVEntry<T> | null>`

Retrieves a value by key. Returns `null` if not found or expired.

#### `del<T>(key: string): Promise<void>`

Deletes a key.

#### `list<T>(options?: ListOptions): Promise<KVEntry<T>[]>`

Lists keys matching the criteria.

- `options.prefix`: Filter keys starting with this prefix.
- `options.limit`: Max number of results.
- `options.cursor`: (Not yet implemented)
- `options.where`: Filter by JSON field value.
- `options.reverse`: Reverse sort order.

#### `transaction(mode?: "write" | "read" | "deferred"): Promise<KVTransaction>`

Starts a new transaction. Returns a `KVTransaction` instance which has the same methods as `LibSQLKV` plus `commit()`, `rollback()`, and `close()`.
