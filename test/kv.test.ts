import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { createKV, LibSQLKV } from "../src/index";

const url = process.env.VITE_DB_URL;
const authToken = process.env.VITE_DB_AUTH_TOKEN;

describe.runIf(!!url && !!authToken)("LibSQLKV", () => {
  let kv: LibSQLKV;

  beforeAll(async () => {
    kv = await createKV({
      url: url!,
      authToken: authToken!,
    });
  });

  afterAll(() => {
    kv.close();
  });

  beforeEach(async () => {
    // Clean up all keys before each test to ensure isolation
    const entries = await kv.list();
    await Promise.all(entries.map((entry) => kv.del(entry.key)));
  });

  it("should set and get a string value", async () => {
    await kv.set("test-string", "hello world");
    const entry = await kv.get("test-string");
    expect(entry).not.toBeNull();
    expect(entry?.key).toBe("test-string");
    expect(entry?.value).toBe("hello world");
    expect(entry?.version).toBeDefined();
  });

  it("should set and get a number value", async () => {
    await kv.set("test-number", 12_345);
    const entry = await kv.get<number>("test-number");
    expect(entry?.value).toBe(12_345);
  });

  it("should set and get a boolean value", async () => {
    await kv.set("test-boolean", true);
    const entry = await kv.get<boolean>("test-boolean");
    expect(entry?.value).toBe(true);
  });

  it("should set and get a JSON value", async () => {
    const data = { foo: "bar", baz: 123 };
    await kv.set("test-json", data);
    const entry = await kv.get("test-json");
    expect(entry?.value).toEqual(data);
  });

  it("should set and get a binary value (Uint8Array)", async () => {
    const data = new Uint8Array([1, 2, 3, 4]);
    await kv.set("test-binary", data);
    const entry = await kv.get<Uint8Array>("test-binary");
    expect(entry?.value).toEqual(data);
  });

  it("should return null for non-existent key", async () => {
    const entry = await kv.get("non-existent");
    expect(entry).toBeNull();
  });

  it("should delete a key", async () => {
    await kv.set("to-delete", "value");
    await kv.del("to-delete");
    const entry = await kv.get("to-delete");
    expect(entry).toBeNull();
  });

  it("should get many keys", async () => {
    await Promise.all([
      kv.set("key1", "value1"),
      kv.set("key2", "value2"),
      kv.set("key3", "value3"),
    ]);

    const entries = await kv.getMany(["key1", "key3", "non-existent"]);
    expect(entries).toHaveLength(3);
    expect(entries[0]?.value).toBe("value1");
    expect(entries[1]?.value).toBe("value3");
    expect(entries[2]).toBeNull();
  });

  it("should return empty array for getMany with no keys", async () => {
    const entries = await kv.getMany([]);
    expect(entries).toEqual([]);
  });

  it("should handle expiration", async () => {
    await kv.set("expired", "value", { expireIn: 2000 }); // 2s

    // Verify it exists initially
    let entry = await kv.get("expired");
    expect(entry?.value).toBe("value");

    // Wait for expiration
    await new Promise((resolve) => setTimeout(resolve, 2500));

    // Verify it is gone
    entry = await kv.get("expired");
    expect(entry).toBeNull();
  });

  it("should not list expired keys", async () => {
    await Promise.all([
      kv.set("valid", "val"),
      kv.set("expired", "val", { expireIn: 1000 }),
    ]);

    // Wait for expiration
    await new Promise((resolve) => setTimeout(resolve, 1500));

    const entries = await kv.list();
    const keys = entries.map((e) => e.key);
    expect(keys).toContain("valid");
    expect(keys).not.toContain("expired");
  });

  it("should list keys with prefix", async () => {
    await Promise.all([
      kv.set("prefix:1", "val1"),
      kv.set("prefix:2", "val2"),
      kv.set("other:1", "val3"),
    ]);

    const entries = await kv.list({ prefix: "prefix:" });
    expect(entries).toHaveLength(2);
    const keys = entries.map((e) => e.key);
    expect(keys).toContain("prefix:1");
    expect(keys).toContain("prefix:2");
  });

  it("should list keys with limit", async () => {
    await Promise.all([
      kv.set("a", 1),
      kv.set("b", 2),
      kv.set("c", 3),
    ]);

    const entries = await kv.list({ limit: 2 });
    expect(entries).toHaveLength(2);
  });

  it("should list all keys if no options provided", async () => {
    await Promise.all([
      kv.set("a", 1),
      kv.set("b", 2),
    ]);
    const entries = await kv.list();
    expect(entries).toHaveLength(2);
  });

  it("should list keys in reverse order", async () => {
    await Promise.all([
      kv.set("a", 1),
      kv.set("b", 2),
    ]);

    const entries = await kv.list({ reverse: true });
    // Note: list returns all keys if no limit, but ordered.
    // Since we clean up before each test, we only have a and b.
    expect(entries[0]?.key).toBe("b");
    expect(entries[1]?.key).toBe("a");
  });

  it("should filter list by JSON field", async () => {
    await Promise.all([
      kv.set("user:1", { name: "Alice", age: 30 }),
      kv.set("user:2", { name: "Bob", age: 25 }),
      kv.set("user:3", { name: "Charlie", age: 35 }),
    ])

    const entries = await kv.list({
      where: {
        field: "age",
        operator: ">",
        value: 28,
      },
    });

    expect(entries).toHaveLength(2);
    const names = entries.map((e: any) => e.value.name);
    expect(names).toContain("Alice");
    expect(names).toContain("Charlie");
    expect(names).not.toContain("Bob");
  });

  describe("Transactions", () => {
    it("should commit a transaction", async () => {
      const tx = await kv.transaction();
      await tx.set("tx-key", "tx-value");
      await tx.commit();

      const entry = await kv.get("tx-key");
      expect(entry?.value).toBe("tx-value");
    });

    it("should rollback a transaction", async () => {
      const tx = await kv.transaction();
      await tx.set("tx-rollback", "value");
      await tx.rollback();

      const entry = await kv.get("tx-rollback");
      expect(entry).toBeNull();
    });

    it("should read your writes in a transaction", async () => {
      const tx = await kv.transaction();
      await tx.set("tx-read-write", "value");
      const entry = await tx.get("tx-read-write");
      expect(entry?.value).toBe("value");
      await tx.commit();
    });

    it("should handle expiration in transaction", async () => {
      await kv.set("tx-expired", "value", { expireIn: 1000 });

      // Wait for expiration
      await new Promise((resolve) => setTimeout(resolve, 1500));

      const tx = await kv.transaction();
      const entry = await tx.get("tx-expired");
      expect(entry).toBeNull();
      await tx.commit();

      // Verify it is deleted from main KV too (since tx.get deletes it)
      const mainEntry = await kv.get("tx-expired");
      expect(mainEntry).toBeNull();
    });
  });
});
