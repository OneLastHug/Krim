import { describe, expect, test, vi } from "vitest";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { diffManifests, manifestBytesSum, parseManifest, parseObservationsPage, parseTurnObservations } from "../src/observability/obs-contracts";
import pageFixture from "./observability-api-page-fixture.json";
import detailFixtures from "./observability-api-detail-fixtures.json";
import { readWorkbenchView } from "../src/lib/workbench-route";
import { readObsState, writeObsState } from "../src/observability/obs-route";
import type { ObsRequestRecord } from "../src/observability/obs-contracts";
import { contextRequestSnapshot, useSessionContext, type SessionContextState } from "../src/hooks/use-session-context";
import type { SessionState } from "../src/hooks/use-session";
import { formatCount, formatReportedTokens, formatTokens } from "../src/observability/obs-format";
import { parseTracePage } from "../src/lib/trace-contracts";


test("原Trace轮次深链进入运行诊断，新增观测链接作为同一分区别名", () => {
  const original = new URL("https://krim.test/ui/?view=trace&workspace=team&session=S&turn=T");
  expect(readWorkbenchView(original)).toBe("trace");
  expect(readObsState(original)).toMatchObject({ page: "runs", session: "S", turn: "T" });
  const next = writeObsState(original, { ...readObsState(original), page: "context", request: "request-2" });
  expect(next.searchParams.get("view")).toBe("trace");
  expect(next.searchParams.has("session")).toBe(false);
  expect(readObsState(next)).toMatchObject({ page: "context", session: "S", turn: "T", request: "request-2" });
  const compatibility = new URL("https://krim.test/ui/?view=observe&obs=status&obs_session=S&obs_turn=T");
  expect(readWorkbenchView(compatibility)).toBe("trace");
  expect(readObsState(compatibility)).toMatchObject({ page: "status", session: "S", turn: "T" });
});

/** 来自隔离验收服务（合成脚本运行）的真实响应；不含任何正文。 */

const page = parseObservationsPage(pageFixture);

test("开发目标记录不会使整个观测列表失效", () => {
  const raw = structuredClone(pageFixture);
  raw.items[0]!.target_key = "target-development";
  const before = JSON.stringify(raw);
  expect(parseObservationsPage(raw).items[0]!.target_key).toBe("target-development");
  expect(JSON.stringify(raw)).toBe(before);
  raw.items[0]!.target_key = "unknown-target";
  expect(() => parseObservationsPage(raw)).toThrow();
});

test("兼容 Trace 列表接受正式开发目标而不放宽未知目标", () => {
  const item = page.items[0]!;
  const raw = { items: [{
    source_kind: item.source_kind, run_id: item.run_id, trace_id: item.trace_id,
    session_id: item.session_id, turn_id: item.turn_id, session_title: item.session_title,
    target_key: "target-development", status: item.status,
    created_at: item.created_at, finished_at: item.finished_at,
  }], next_cursor: null };
  expect(parseTracePage(raw).items[0]!.target_key).toBe("target-development");
  raw.items[0]!.target_key = "unknown-target";
  expect(() => parseTracePage(raw)).toThrow();
});

function tokenRequest(id: string, overrides: Partial<ObsRequestRecord> = {}): ObsRequestRecord {
  return { id, step_index: 0, purpose: "task", status: "ok", started_at: 1, ended_at: 2,
    duration_ms: 1000, duration_source: "mono", timing: null, tool_calls: 0, usage: null,
    manifest: null, manifest_status: "missing", cost: null, cost_status: "unpriced", ...overrides };
}
const tokenManifest = parseManifest({ version: 1, counter_version: "heuristic-script/1", input_bytes: 37800,
  estimated_tokens: 9450, model_context_window: 1000000, items_truncated: false, omitted_items: 0,
  source_refs_truncated: false, items: [{ key: "message:0:user", category: "history", label: "User message",
    content_hash: "a".repeat(64), input_bytes: 37800, estimated_tokens: 9450 }] });
test("压缩变化接受前后token计量且兼容旧记录，拒绝非法值和未知字段", () => {
  const change = { pass_index: 0, summary_step_index: 0, before_input_bytes: 269146,
    after_input_bytes: 57092, before_input_tokens: 86781, after_input_tokens: 18506,
    counter_version: "calibrated/heuristic-script/1", source_hash: "a".repeat(64), output_hash: "b".repeat(64) };
  const raw = { ...tokenManifest, compaction_changes: [change] };
  const before = JSON.stringify(raw);
  expect(parseManifest(raw).compaction_changes?.[0]).toEqual(change);
  expect(JSON.stringify(raw)).toBe(before);
  const { before_input_tokens, after_input_tokens, ...legacy } = change;
  expect(parseManifest({ ...raw, compaction_changes: [legacy] }).compaction_changes?.[0]).toEqual(legacy);
  for (const invalid of [-1, 1.5, "18506", Number.NaN]) {
    expect(() => parseManifest({ ...raw, compaction_changes: [{ ...change, after_input_tokens: invalid }] })).toThrow();
  }
  expect(() => parseManifest({ ...raw, compaction_changes: [{ ...change, unrecognized: true }] })).toThrow();
});

const reportedTokenUsage = { usage: { input_tokens: 9123, output_tokens: 2173, total_tokens: 11296,
  cached_input_tokens: 4001, cache_write_input_tokens: 0, reasoning_output_tokens: 432 }, usage_reports: 3, usage_final: true };

test("token 数量保留完整整数，只有预估格式带约等号", () => {
  expect(formatReportedTokens(9123)).toBe("9,123");
  expect(formatReportedTokens(1000001)).toBe("1,000,001");
  expect(formatTokens(9123)).toBe("≈9,123");
  expect(formatTokens(1000001)).toBe("≈1,000,001");
  expect(formatCount(900000)).toBe("900,000");
  expect(formatReportedTokens(0)).toBe("0");
  expect(formatReportedTokens(null)).toBe("未知");
  expect(formatTokens(Number.NaN)).toBe("未知");
});

test("侧栏按同一请求配对预估与实际 usage，忽略之后的压缩摘要请求", () => {
  const request = tokenRequest("task", { manifest: tokenManifest, manifest_status: "present", usage: reportedTokenUsage });
  const summary = tokenRequest("summary", { purpose: "compaction_summary", usage: { ...reportedTokenUsage,
    usage: { ...reportedTokenUsage.usage, input_tokens: 10 } } });
  const result = contextRequestSnapshot([request, summary]);
  expect(result.requestId).toBe("task");
  expect(result.manifest?.estimated_tokens).toBe(9450);
  expect(result.reported).toEqual({ input: 9123, output: 2173, total: 11296, cached: 4001, cacheWrite: 0, reasoning: 432, final: true });
});

test("最新请求缺清单时不拿旧请求清单配对最新 usage", () => {
  const result = contextRequestSnapshot([
    tokenRequest("old", { manifest: tokenManifest, usage: reportedTokenUsage }),
    tokenRequest("new", { usage: reportedTokenUsage }),
  ]);
  expect(result.requestId).toBe("new");
  expect(result.manifest).toBeNull();
  expect(result.reported?.input).toBe(9123);
});

test("最新请求缺 usage 时不回填前一次实际用量或使用预估值冒充", () => {
  const result = contextRequestSnapshot([
    tokenRequest("old", { usage: reportedTokenUsage }),
    tokenRequest("new", { manifest: tokenManifest }),
  ]);
  expect(result.requestId).toBe("new");
  expect(result.manifest).toBe(tokenManifest);
  expect(result.reported).toBeNull();
});

test("部分上报保留未知总量，不把输入输出相加成上游总量", () => {
  const result = contextRequestSnapshot([tokenRequest("partial", { usage: { ...reportedTokenUsage,
    usage_final: false, usage: { ...reportedTokenUsage.usage, output_tokens: null, total_tokens: null } } })]);
  expect(result.reported).toMatchObject({ input: 9123, output: null, total: null, cacheWrite: 0, final: false });
});

test("没有业务请求时不从摘要构造上下文和上报用量", () => {
  expect(contextRequestSnapshot([])).toEqual({ requestId: "", manifest: null, reported: null });
  expect(contextRequestSnapshot([tokenRequest("summary", { purpose: "summary", usage: reportedTokenUsage })]))
    .toEqual({ requestId: "", manifest: null, reported: null });
});

test("真实列表页通过解析：浮点毫秒、完整状态与scope counted=returned_page", () => {
  expect(page.items.length).toBe(5);
  expect(page.scope.counted).toBe("returned_page");
  const statuses = new Set(page.items.map(item => item.status));
  expect(statuses.has("failed")).toBe(true);
  expect(statuses.has("completed")).toBe(true);
  for (const item of page.items) {
    if (item.duration_ms !== null) expect(Number.isFinite(item.duration_ms)).toBe(true);
    expect(item.cost).toBeNull();
    expect(item.cost_status).toBe("unpriced");
  }
});

describe("真实轮次详情", () => {
  const fixtures = detailFixtures as { item_status: string; status: number; body: unknown }[];

  test("解析清单不修改输入对象，重复解析结果相同", () => {
    for (const fixture of fixtures) {
      const body = fixture.body as { requests: { manifest: unknown }[] };
      for (const request of body.requests) {
        if (request.manifest === null) continue;
        const before = JSON.stringify(request.manifest);
        const first = parseManifest(request.manifest);
        expect(JSON.stringify(request.manifest)).toBe(before);
        expect(parseManifest(request.manifest)).toEqual(first);
        expect(parseManifest(first)).toEqual(first);
      }
    }
  });

  test("failed 与 completed 轮次均通过 parseTurnObservations", () => {
    for (const fixture of fixtures) {
      expect(fixture.status).toBe(200);
      const matching = page.items.find(item => (fixture.body as { session_id: string; turn_id: string }).session_id === item.session_id
        && (fixture.body as { session_id: string; turn_id: string }).turn_id === item.turn_id)!;
      const before = JSON.stringify(fixture.body);
      const detail = parseTurnObservations(fixture.body, matching);
      expect(JSON.stringify(fixture.body)).toBe(before);
      for (const request of detail.requests) {
        for (const entry of request.manifest?.items ?? []) {
          expect(entry.message_index === null || Number.isInteger(entry.message_index)).toBe(true);
          expect(entry.tool_index === null || Number.isInteger(entry.tool_index)).toBe(true);
        }
      }
      expect(detail.session_id).toBe(matching.session_id);
      expect(detail.cost).toBeNull();
    }
  });

  test("超过64次压缩的详情仍解析，并保留逐条数值校验", () => {
    const body = JSON.parse(JSON.stringify(fixtures[0]!.body)) as {
      session_id: string; turn_id: string; compactions: Record<string, unknown>[];
    };
    const matching = page.items.find(item => item.session_id === body.session_id && item.turn_id === body.turn_id)!;
    body.compactions = Array.from({ length: 96 }, (_, pass_index) => ({
      pass_index, status: "complete", created_at: 1, finished_at: 2, summary_step_index: pass_index,
      covered_turns: 1, covered_turn_ids: ["turn-source"], source_hash: "a".repeat(64),
      output_hash: "b".repeat(64), summary_bytes: 100, counter_version: "approx-utf8/1", reserved_output_tokens: 2048,
    }));
    expect(parseTurnObservations(body, matching).compactions).toHaveLength(96);
    body.compactions[95]!.summary_bytes = 1.5;
    expect(() => parseTurnObservations(body, matching)).toThrow("压缩记录数值无效");
    body.compactions[95]!.summary_bytes = 100;
    delete body.compactions[95]!.status;
    expect(() => parseTurnObservations(body, matching)).toThrow("压缩记录结构无效");
  });

  test("真实 manifest：分项字节守恒、总量与 items_truncated 可解析，差分保持纯计算", () => {
    for (const fixture of fixtures) {
      const body = fixture.body as { requests: { manifest: unknown; manifest_status: string }[] };
      for (const request of body.requests) {
        if (request.manifest === null) continue;
        const manifest = parseManifest(request.manifest);
        expect(manifest.version).toBe(1);
        expect(typeof manifest.source_refs_truncated).toBe("boolean");
        if (!manifest.items_truncated) {
          expect(manifestBytesSum(manifest)).toBe(manifest.input_bytes);
        }
        const tokenSum = manifest.items.reduce((sum, item) => sum + (item.estimated_tokens ?? 0), 0);
        expect(tokenSum).toBe(manifest.estimated_tokens);
      }
      const manifests = body.requests.map(request => request.manifest === null ? null : parseManifest(request.manifest)).filter((value): value is NonNullable<typeof value> => value !== null);
      if (manifests.length >= 2) {
        const rows = diffManifests(manifests[0]!, manifests[1]!);
        expect(rows.length).toBeGreaterThanOrEqual(0);
      }
    }
  });

  test("无用量记录的轮次保持 usage_final:null、totals:null，不以零补齐", () => {
    const fixture = fixtures.find(entry => (entry.body as { usage: { totals: unknown } }).usage.totals === null);
    expect(fixture).toBeTruthy();
    const detail = parseTurnObservations(fixture!.body, page.items.find(item => item.turn_id === (fixture!.body as { turn_id: string }).turn_id)!);
    expect(detail.usage.totals).toBeNull();
    expect(detail.usage.usage_complete).toBe(false);
  });
});

// 侧栏上下文用量：运行中的当前轮优先、事件边界刷新、身份切换中止（renderless hook 探针）。

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

interface HookChatParts { id?: string; generation?: number; history?: { id: string; status: string; run_id: string }[]; currentTurn?: { id: string; status: string; run_id: string } | null; events?: { kind: string }[] }
function hookChat(parts: HookChatParts): SessionState {
  const id = parts.id ?? "A";
  return {
    ready: true, actor: { principal_id: "local", generation: parts.generation ?? 1 }, workspace: "local", id,
    session: { id, context_policy: { compaction_trigger_tokens: 900000, max_tokens: null, reserved_output_tokens: 0, counter_version: "approx-utf8/1" } },
    history: parts.history ?? [], currentTurn: parts.currentTurn ?? null, events: parts.events ?? [], reading: false,
  } as unknown as SessionState;
}
const hookDetail = (sessionId: string, turnId: string, tokens?: number) => {
  const raw = JSON.stringify((detailFixtures as { body: unknown }[])[0]!.body)
    .replace(/"session_id":"[^"]*"/g, `"session_id":"${sessionId}"`)
    .replace(/"turn_id":"[^"]*"/g, `"turn_id":"${turnId}"`)
    .replace(/"estimated_tokens":\d+/g, `"estimated_tokens":${tokens ?? 9450}`);
  return new Response(raw, { status: 200, headers: { "Content-Type": "application/json" } });
};
async function withContextProbe(initial: SessionState, respond: (path: string, signal?: AbortSignal | null) => Response | Promise<Response>, run: (control: { latest: () => SessionContextState; calls: { path: string; signal?: AbortSignal | null }[]; rerender: (chat: SessionState) => Promise<void>; settle: () => Promise<void>; until: (check: () => void) => Promise<void> }) => Promise<void>) {
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  const original = globalThis.fetch;
  const holder: { chat: SessionState } = { chat: initial };
  const calls: { path: string; signal?: AbortSignal | null }[] = [];
  let latest: SessionContextState | null = null;
  function Probe() { latest = useSessionContext(holder.chat, "key", true); return null; }
  globalThis.fetch = async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input);
    if (path.endsWith("/observations")) calls.push({ path, signal: options.signal });
    return respond(path, options.signal);
  };
  const until = async (check: () => void) => {
    let error: unknown;
    for (let i = 0; i < 60; i++) {
      try { check(); return; } catch (value) { error = value; await act(async () => { await new Promise(r => setTimeout(r, 0)); }); }
    }
    throw error;
  };
  try {
    await act(async () => root.render(createElement(Probe)));
    await run({
      latest: () => latest!, calls,
      rerender: chat => act(async () => { holder.chat = chat; root.render(createElement(Probe)); }),
      settle: () => act(async () => { await new Promise(r => setTimeout(r, 0)); }),
      until,
    });
  } finally {
    await act(async () => root.unmount());
    container.remove();
    globalThis.fetch = original;
    vi.unstubAllGlobals();
  }
}
const runningTurn = (id = "T2") => ({ id, status: "running", run_id: "r-" + id });

test("上下文用量优先读运行中的当前轮，按model_start/usage边界刷新，结束后读最终一次", async () => {
  const done = hookChat({ history: [{ id: "T1", status: "failed", run_id: "r-T1" }, { id: "T2", status: "completed", run_id: "r-T2" }], currentTurn: null });
  await withContextProbe(
    hookChat({ history: [{ id: "T1", status: "failed", run_id: "r-T1" }], currentTurn: runningTurn(), events: [{ kind: "run_started" }, { kind: "model_start" }] }),
    path => hookDetail(path.match(/sessions\/([^/]+)\/turns/)![1]!, path.match(/turns\/([^/]+)\/observations/)![1]!),
    async ({ latest, calls, rerender, settle, until }) => {
      await until(() => expect(calls).toHaveLength(1));
      expect(calls[0]!.path).toContain("/turns/T2/observations");
      expect(latest().turnId).toBe("T2");
      await until(() => expect(latest().used).not.toBeNull());
      expect(latest().used).toBe(latest().manifest?.estimated_tokens);
      expect(latest().limit).toBe(900000);
      await rerender(hookChat({ history: [{ id: "T1", status: "failed", run_id: "r-T1" }], currentTurn: runningTurn(), events: [{ kind: "run_started" }, { kind: "model_start" }, { kind: "usage" }, { kind: "text" }] }));
      await until(() => expect(calls).toHaveLength(2));
      await rerender(hookChat({ history: [{ id: "T1", status: "failed", run_id: "r-T1" }], currentTurn: runningTurn(), events: [{ kind: "run_started" }, { kind: "model_start" }, { kind: "usage" }, { kind: "text" }, { kind: "text" }] }));
      await settle();
      expect(calls).toHaveLength(2);
      await rerender(done);
      await until(() => expect(calls).toHaveLength(3));
      expect(latest().turnId).toBe("T2");
    },
  );
});

test("同轮边界刷新在请求前后保留快照，新响应到达才更新数字", async () => {
  let release!: (response: Response) => void;
  const held = new Promise<Response>(resolve => { release = resolve; });
  let reads = 0;
  await withContextProbe(
    hookChat({ currentTurn: runningTurn(), events: [{ kind: "model_start" }] }),
    () => ++reads === 1 ? hookDetail("A", "T2", 111) : held,
    async ({ latest, rerender, until, settle }) => {
      await until(() => expect(latest().used).toBe(111));
      const snapshot = latest().data;
      await rerender(hookChat({ currentTurn: runningTurn(), events: [{ kind: "model_start" }, { kind: "usage" }] }));
      expect(latest().loading).toBe(true);
      expect(latest().data).toBe(snapshot);
      expect(latest().used).toBe(111);
      await settle();
      expect(latest().used).toBe(111);
      await act(async () => release(hookDetail("A", "T2", 222)));
      await until(() => expect(latest().used).toBe(222));
      expect(latest().loading).toBe(false);
    },
  );
});

test("切换会话中止在途读取，不把旧轮快照配到新会话", async () => {
  let release!: (response: Response) => void;
  const held = new Promise<Response>(resolve => { release = resolve; });
  await withContextProbe(
    hookChat({ currentTurn: runningTurn(), events: [{ kind: "model_start" }] }),
    path => path.endsWith("/sessions/A/turns/T2/observations") ? held : hookDetail(path.match(/sessions\/([^/]+)\/turns/)![1]!, path.match(/turns\/([^/]+)\/observations/)![1]!),
    async ({ latest, calls, rerender, until }) => {
      await until(() => expect(calls).toHaveLength(1));
      expect(latest().turnId).toBe("T2");
      await rerender(hookChat({ id: "B", history: [{ id: "U1", status: "running", run_id: "r-U1" }], currentTurn: runningTurn("U1"), events: [{ kind: "model_start" }] }));
      await until(() => expect(calls.some(call => call.path.endsWith("/sessions/B/turns/U1/observations"))).toBe(true));
      expect(calls[0]!.signal?.aborted).toBe(true);
      await act(async () => release(hookDetail("B", "U1")));
      await until(() => expect(latest().turnId).toBe("U1"));
    },
  );
});

test("同轮重新登录中止旧读取且不沿用旧身份快照", async () => {
  let release!: (response: Response) => void;
  const held = new Promise<Response>(resolve => { release = resolve; });
  let reads = 0;
  await withContextProbe(
    hookChat({ currentTurn: runningTurn(), events: [{ kind: "model_start" }] }),
    (_path, _signal) => {
      reads++;
      return reads === 1 ? held : hookDetail("A", "T2", 222);
    },
    async ({ latest, calls, rerender, settle, until }) => {
      await until(() => expect(calls).toHaveLength(1));
      await rerender(hookChat({ generation: 2, currentTurn: runningTurn(), events: [{ kind: "model_start" }] }));
      await until(() => expect(calls).toHaveLength(2));
      expect(calls[0]!.signal?.aborted).toBe(true);
      await until(() => expect(latest().used).toBe(222));
      await act(async () => release(hookDetail("A", "T2", 111)));
      await settle();
      expect(latest().used).toBe(222);
    },
  );
});
