import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

type Value = null | boolean | number | string | Value[] | { [key: string]: Value };
type RecordValue = { [key: string]: Value };
type Vector = {
  name: string;
  model: "frame" | "result";
  valid: boolean;
  input: RecordValue;
  expected?: RecordValue;
};

function checkJson(value: Value, depth = 0): void {
  assert(depth <= 32);
  if (typeof value === "number") {
    assert(Number.isFinite(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER);
  }
  if (Array.isArray(value)) {
    for (const child of value) checkJson(child, depth + 1);
  } else if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) checkJson(child, depth + 1);
  }
  if (depth === 0) assert(Buffer.byteLength(JSON.stringify(value), "utf8") <= 65536);
}

function boundedString(value: Value | undefined): void {
  assert(typeof value === "string" && [...value].length >= 1 && [...value].length <= 128);
}

function normalize(vector: Vector): RecordValue {
  const value = vector.input;
  const output: RecordValue = {};
  if (vector.model === "result") {
    assert(Object.keys(value).every((key) => ["ok", "data", "error"].includes(key)));
    assert(typeof value.ok === "boolean");
    if (value.ok) assert(value.error === undefined || value.error === null);
    else {
      assert(typeof value.error === "string" && value.error.length > 0);
      assert(value.data === undefined || value.data === null);
    }
    checkJson(value.data ?? null);
  } else {
    const keys = ["protocol_version", "kind", "id", "channel", "data", "stream_id", "seq"];
    assert(Object.keys(value).every((key) => keys.includes(key)));
    assert(("protocol_version" in value ? value.protocol_version : 2) === 2);
    assert(typeof value.kind === "string");
    assert(["call", "reply", "notify", "chunk", "end", "cancel"].includes(value.kind));
    boundedString(value.id);
    boundedString(value.channel);
    if (value.stream_id != null) boundedString(value.stream_id);
    if (value.seq != null) assert(Number.isSafeInteger(value.seq) && (value.seq as number) >= 0);
    if (["chunk", "end"].includes(value.kind)) assert(value.stream_id != null && value.seq != null);
    checkJson(value.data ?? null);
    output.protocol_version = 2;
  }
  for (const [key, item] of Object.entries(value)) if (item !== null) output[key] = item;
  if (output.seq === 0) output.seq = 0;
  return output;
}

const vectors: Vector[] = JSON.parse(
  readFileSync(new URL("../../tests/contracts/vectors.json", import.meta.url), "utf8"),
);
for (const vector of vectors) {
  if (vector.valid) assert.deepEqual(normalize(vector), vector.expected, vector.name);
  else assert.throws(() => normalize(vector), undefined, vector.name);
}
console.log(`Krim Agent TypeScript contract vectors: ${vectors.length} passed`);
