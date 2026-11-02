import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { request } from "../src/api";
import { ToolCatalog } from "../src/components/ToolCatalog";

vi.mock("../src/api", () => ({ request: vi.fn() }));

let root: Root;
let host: HTMLDivElement;
let mounted = false;
const readCatalog = vi.mocked(request);
const baseScope = { principal_id: "operator", generation: 3, workspace: "local", session: "session-A", target: "target-http" };
const baseProps = { apiKey: "synthetic-key-A", targetKey: "target-http", scope: baseScope, active: true };

function externalTool(name: string, kind: "http" | "mcp" = "http", risk: "read" | "write" = "read", server = "service", tool = "remote_tool") {
  return {
    name, version: "1", implementation_digest: "a".repeat(64),
    input_schema: { type: "object", properties: { query: { type: "string" } } },
    output_schema: { type: "object", properties: { result: { type: "string" } } },
    risk, source: { kind, server, tool, configuration_digest: "b".repeat(64) },
    binding: { target_name: "demo", target_hash: "c".repeat(64) }, fingerprint: "d".repeat(64),
  };
}

beforeEach(() => {
  readCatalog.mockReset();
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  mounted = true;
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
});

afterEach(async () => {
  if (mounted) await act(async () => root.unmount());
  host.remove();
});

async function render(props = baseProps) {
  await act(async () => {
    root.render(<ToolCatalog {...props} />);
    await Promise.resolve();
    await Promise.resolve();
  });
}

test("旧工具没有实际来源时不显示目录", async () => {
  readCatalog.mockResolvedValue([{
    name: "legacy_tool", version: "1", implementation_digest: "a".repeat(64),
    input_schema: {}, output_schema: {}, fingerprint: "b".repeat(64),
  }]);

  await render();

  expect(readCatalog).toHaveBeenCalledWith("/v2/targets/target-http/tools", "synthetic-key-A", undefined, expect.any(AbortSignal));
  expect(host.querySelector('[aria-label="外部工具目录"]')).toBeNull();
  expect(host.textContent).not.toContain("legacy_tool");
});

test("HTTP 与 MCP 显示真实来源、风险和可折叠 Schema", async () => {
  readCatalog.mockResolvedValue([
    externalTool("web_search", "http", "read", "tavily", "search"),
    externalTool("repo_update", "mcp", "write", "engineering", "apply_patch"),
  ]);
  const props = { ...baseProps, targetKey: "target/alpha", scope: { ...baseScope, target: "target/alpha" } };

  await render(props);

  expect(readCatalog).toHaveBeenCalledWith("/v2/targets/target%2Falpha/tools", "synthetic-key-A", undefined, expect.any(AbortSignal));
  expect(host.textContent).toContain("web_search → tavily / search");
  expect(host.textContent).toContain("repo_update → engineering / apply_patch");
  expect(host.textContent).toContain("HTTP　只读");
  expect(host.textContent).toContain("MCP　写操作");
  const schema = host.querySelector("article details");
  expect(schema).toBeTruthy();
  expect((schema as HTMLDetailsElement).open).toBe(false);
  await act(async () => (schema!.querySelector("summary") as HTMLElement).click());
  expect((schema as HTMLDetailsElement).open).toBe(true);
  expect(schema?.textContent).toContain('"query"');
  expect(schema?.textContent).toContain('"result"');
});

test("恶意名称、来源与 Schema 只作为纯文本显示", async () => {
  readCatalog.mockResolvedValue([{
    ...externalTool('<img src=x onerror="alert(1)">', "http", "read", "<script>服务</script>", '" onmouseover="bad()'),
    input_schema: { description: "<svg onload=alert(1)>" },
  }]);

  await render();

  expect(host.querySelector("img, script, svg")).toBeNull();
  expect(host.textContent).toContain('<img src=x onerror="alert(1)">');
  expect(host.textContent).toContain("<script>服务</script>");
  expect(host.textContent).toContain("<svg onload=alert(1)>");
});

test("身份与目标切换时迟到的旧目录不会污染新目录", async () => {
  const pending: { key: string; signal?: AbortSignal; resolve: (value: unknown) => void }[] = [];
  readCatalog.mockImplementation((_path, key, _body, signal) => new Promise(resolve => {
    pending.push({ key, signal, resolve });
  }));

  await render();
  await render({
    apiKey: "synthetic-key-B",
    targetKey: "target-mcp",
    scope: { principal_id: "another-operator", generation: 4, workspace: "research", session: "session-B", target: "target-mcp" },
    active: true,
  });

  expect(pending).toHaveLength(2);
  expect(pending[0]!.key).toBe("synthetic-key-A");
  expect(pending[0]!.signal?.aborted).toBe(true);
  expect(pending[1]!.key).toBe("synthetic-key-B");
  await act(async () => {
    pending[0]!.resolve([externalTool("stale_from_old_identity")]);
    pending[1]!.resolve([externalTool("current_from_new_identity", "mcp", "read")]);
    await Promise.resolve();
  });

  expect(host.textContent).toContain("current_from_new_identity");
  expect(host.textContent).not.toContain("stale_from_old_identity");
});

test("卸载组件会中止目录读取", async () => {
  let signal: AbortSignal | undefined;
  readCatalog.mockImplementation((_path, _key, _body, requestSignal) => {
    signal = requestSignal;
    return new Promise(() => undefined);
  });

  await render();
  expect(signal?.aborted).toBe(false);
  await act(async () => root.unmount());
  mounted = false;
  expect(signal?.aborted).toBe(true);
});
