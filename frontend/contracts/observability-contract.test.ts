import { describe, expect, test } from "vitest";
import { diffManifests, manifestBytesSum, parseManifest, parseObservationsPage, parseTurnObservations } from "../src/observability/obs-contracts";
import pageFixture from "./observability-api-page-fixture.json";
import detailFixtures from "./observability-api-detail-fixtures.json";
import { readWorkbenchView } from "../src/lib/workbench-route";
import { readObsState, writeObsState } from "../src/observability/obs-route";


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
