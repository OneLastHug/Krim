import assert from "node:assert/strict";
import {
  artifactPath, definitelyNotAdmitted, inputRequestHash, modelArguments, parseArtifact,
  parseArtifactChunk, parseEffect, parseInputSnapshot, parseWorkspaceTurn, parseVerification, parseSummary, parsePlan,
} from "../src/lib/workspace-contracts.ts";

const hash = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
const uuid = "11111111-1111-4111-8111-111111111111";
const file = {path: "你好.txt", encoding: "base64", data: "aGVsbG8=", sha256: hash};
const input = {schema_version: 1, request_key: "unicode", expected_head_revision: 0, files: [file]};
const original = JSON.stringify(input);
assert.equal((await parseInputSnapshot(input)).files[0]!.path, "你好.txt");
assert.equal(await inputRequestHash(input), "66eaffed680357956a85490cdeee689f771a91194a173ed6831923e1bb3266bc");
assert.equal(JSON.stringify(input), original);
for (const bad of [
  {...input, expected_head_revision: true}, {...input, files: [file, file]},
  {...input, files: [{...file, sha256: "0".repeat(64)}]},
  {...input, files: [{...file, data: "aGVsbG8=\n"}]},
  {...input, files: [{...file, path: "../x"}]},
  {...input, files: [{...file, path: ".env.production"}]},
  {...input, files: [{...file, path: "e\u0301.txt"}]},
  {...input, secret: "must not accept host metadata"},
]) await assert.rejects(() => parseInputSnapshot(bad));
const bytes = Buffer.alloc(32769, 120), crypto = await import("node:crypto");
await assert.rejects(() => parseInputSnapshot({...input, files: [{
  path: "large", encoding: "base64", data: bytes.toString("base64"),
  sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
}]}));
for (const path of ["/x", "../x", "x//y", ".ssh/key", "a/../b", "a\\b", "a/".repeat(17)+"x"]) assert.throws(() => artifactPath(path));
const turn = {schema_version: 1, request_key: "t", text: "做一个小项目", expected_revision: 0,
  input: {mode: "head_at_dispatch", revision_id: null},
  completion: {profile: "python-unittest/1", required_paths: ["你好.txt"], conditions: [
    {id: "json", kind: "json_equal", path: "data.json", pointer: "/value", expected: false, symbols: []},
    {id: "null", kind: "json_equal", path: "data.json", pointer: "/other", expected: null, symbols: []},
  ]}};
assert.equal(parseWorkspaceTurn(turn).completion.conditions[0]!.expected, false);
assert.throws(() => parseWorkspaceTurn({...turn, expected_revision: false}));
assert.throws(() => parseWorkspaceTurn({...turn, text: "x".repeat(32769)}));
assert.throws(() => parseWorkspaceTurn({...turn, text: "\u0000".repeat(11000)})); // encoded >64KiB
assert.throws(() => modelArguments({value: "x".repeat(4096)}));
assert.equal(modelArguments({value: false}), '{"value":false}');
const artifact = {revision_id: uuid, session_id: uuid, parent_id: null, revision: 0,
  source_kind: "input", request_key: "a", request_hash: hash, manifest: [{path: "a", size: 5, sha256: hash}],
  manifest_hash: hash, producer_run_id: null, producer_call_id: null};
assert.equal(parseArtifact(artifact).revision, 0);
assert.throws(() => parseArtifact({...artifact, revision: true}));
assert.throws(() => parseArtifact({...artifact, source_kind: "effect"}));
assert.throws(() => parseArtifact({...artifact, host_path: "/secret"}));
assert.equal(parseArtifactChunk({revision_id: uuid, path: "a", offset: 0, total_size: 5,
  length: 5, sha256: hash, data: "aGVsbG8="}).length, 5);
assert.throws(() => parseArtifactChunk({revision_id: uuid, path: "a", offset: 0, total_size: 5,
  length: 6, sha256: hash, data: "aGVsbG8="}));
const effect = {call_id: uuid, kind: "command", phase: "unknown", executor_started: null,
  result_available: null, exit_code: null, output_complete: null, cleanup: "unknown",
  artifact_revision_id: null, code: "execution_unknown"};
assert.equal(parseEffect(effect).executor_started, null);
assert.throws(() => parseEffect({...effect, executor_started: 1}));
assert.throws(() => parseEffect({...effect, exit_code: false}));
assert.throws(() => parseEffect({...effect, pid: 123}));
assert.equal(definitelyNotAdmitted(true, "input", 409, "workspace_busy"), true);
assert.equal(definitelyNotAdmitted(true, "turn", 409, "workspace_busy"), false);
assert.equal(definitelyNotAdmitted(false, "input", 401, "unauthorized"), false);
for (const code of ["key_conflict", "artifact_conflict", "execution_unknown"])
  assert.equal(definitelyNotAdmitted(true, "input", 409, code), false);
console.log(JSON.stringify({kind: "D1_DATA-pure-contract-vectors", passed: true,
  no_fetch_no_ui_no_effect: true, shared_utf8_request_hash: await inputRequestHash(input)}));

assert.equal(parseVerification({state: "unknown", revision_id: null, command_call_id: null, results: []}).state, "unknown");
assert.throws(() => parseVerification({state: "done", revision_id: null, command_call_id: null, results: []}));
assert.equal(parseSummary({pass_index: 0, status: "completed", source_digest: hash, model_step: 0,
  summary_hash: hash, summary: "不可信历史摘要"}).status, "completed");
assert.throws(() => parseSummary({pass_index: false, status: "completed", source_digest: hash, model_step: 0,
  summary_hash: hash, summary: "x"}));
assert.throws(() => parseSummary({pass_index: 0, status: "completed", source_digest: hash, model_step: 0,
  summary_hash: hash, summary: "中".repeat(683)}));
assert.equal(parsePlan({revision: 0, steps: []}).revision, 0);
assert.throws(() => parsePlan({revision: true, steps: []}));
