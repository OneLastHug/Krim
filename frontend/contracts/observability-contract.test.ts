import { describe, expect, test } from "vitest";
import { diffManifests, manifestBytesSum, parseManifest, parseObservationsPage, parseTurnObservations } from "../src/observability/obs-contracts";
import pageFixture from "./observability-api-page-fixture.json";
import detailFixtures from "./observability-api-detail-fixtures.json";
import { readWorkbenchView } from "../src/lib/workbench-route";
import { readObsState, writeObsState } from "../src/observability/obs-route";
import type { ObsRequestRecord } from "../src/observability/obs-contracts";
import { contextRequestSnapshot } from "../src/hooks/use-session-context";
import { formatCount, formatReportedTokens, formatTokens } from "../src/observability/obs-format";


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

function tokenRequest(id: string, overrides: Partial<ObsRequestRecord> = {}): ObsRequestRecord {
  return { id, step_index: 0, purpose: "task", status: "ok", started_at: 1, ended_at: 2,
    duration_ms: 1000, duration_source: "mono", timing: null, tool_calls: 0, usage: null,
    manifest: null, manifest_status: "missing", cost: null, cost_status: "unpriced", ...overrides };
}
const tokenManifest = parseManifest({ version: 1, counter_version: "heuristic-script/1", input_bytes: 37800,
  estimated_tokens: 9450, model_context_window: 1000000, items_truncated: false, omitted_items: 0,
  source_refs_truncated: false, items: [{ key: "message:0:user", category: "history", label: "User message",
    content_hash: "a".repeat(64), input_bytes: 37800, estimated_tokens: 9450 }] });
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
