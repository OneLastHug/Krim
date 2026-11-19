import { expect, test, vi, afterEach } from "vitest";
import { parseProject, parseFileData } from "../src/lib/development-contracts";
import { fetchFileBytes } from "../src/hooks/use-development";
import { webcrypto, createHash } from "node:crypto";
afterEach(() => vi.unstubAllGlobals());
const project = { project_id: "p", environment_id: "e", principal_id: "owner", workspace_id: "local", title: "Project", generation: 0, status: "unprovisioned", created_at: 1790000000.123456, updated_at: 1790000000.987654 };
const file = (data: string, digest: string) => ({ path: "a.txt", offset: 0, size: data.length, eof: true, sha256: digest, stat_token: [1048592, 922, data.length, 1790000000123456789, 1790000000123456790], data: btoa(data), project_id: "p", generation: 0, may_change: true });
test("accepts real SQLite fractional project timestamps", () => { expect(parseProject(project).created_at).toBe(project.created_at); expect(() => parseProject({ ...project, updated_at: NaN })).toThrow(); });
test("accepts Linux nanosecond stat fields beyond safe integer without rejecting files", () => { expect(parseFileData(file("abc", "a".repeat(64))).stat_token).toHaveLength(5); });
test("assembled file bytes must match the server's full digest", async () => {
  vi.stubGlobal("crypto", webcrypto);
  const bytes = "real-file-bytes", digest = createHash("sha256").update(bytes).digest("hex");
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(file(bytes, digest)))));
  expect(new TextDecoder().decode((await fetchFileBytes("", "p", "a.txt")).bytes)).toBe(bytes);
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(file(bytes, "0".repeat(64))))));
  await expect(fetchFileBytes("", "p", "a.txt")).rejects.toThrow("文件内容校验失败");
});
