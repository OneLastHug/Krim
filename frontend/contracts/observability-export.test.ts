import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, test } from "vitest";
import { OverviewView } from "../src/observability/overview-view";
import { SessionView } from "../src/observability/session-view";
import { RunsView } from "../src/observability/runs-view";
import { parseTurnObservations } from "../src/observability/obs-contracts";
import detailFixtures from "./observability-api-detail-fixtures.json";
import type { ObsViewProps } from "../src/observability/obs-ui";
import { observationCsv } from "../src/observability/export";
import type { ObservationsIndexItem } from "../src/observability/obs-contracts";

const row = {
  session_id: "session", turn_id: "turn", run_id: "run", session_title: '=IMPORTDATA("https://example.invalid")',
  status: "completed", created_at: 1.25, model_requests: 1, tool_calls: 0,
  usage: null, usage_complete: false, duration_ms: 1.25, cost: null, cost_status: "unpriced",
} as ObservationsIndexItem;

test("CSV only serializes the selected authorized page with USD and unknown price fields", () => {
  const csv = observationCsv("team", [row]);
  expect(csv).toContain('"cost_usd","display_currency","cost_status"');
  expect(csv).toContain('"1.25","","USD","unpriced","","",""');
  expect(csv).toContain('"1970-01-01T00:00:01.250Z"');
  expect(csv).toContain('"returned_page"');
  expect(csv).toContain('"\'=IMPORTDATA(""https://example.invalid"")"');
  expect(csv.split("\r\n")).toHaveLength(3);
  expect(csv).not.toContain('"undefined"');
  expect(csv).not.toContain('"null"');
});

test("missing provider usage stays unknown while known durations remain in percentiles", () => {
  const item = { ...row, duration_ms: 125.5, projection_status: "ok", manifest_status: "present" };
  const props = { data: { ready: true, loading: false, error: "", items: [item] }, state: {}, onState() {} } as unknown as ObsViewProps;
  const overview = renderToStaticMarkup(createElement(OverviewView, props));
  const session = renderToStaticMarkup(createElement(SessionView, props));
  expect(overview).not.toContain("≈0");
  expect(session).not.toContain("≈0");
  expect(overview).toContain("126 ms");
  expect(overview).toContain("1 个有效样本");
  expect(overview).toContain("未知");
  expect(session).toContain("未知");
});

test("reported input/output are not recomputed into a missing provider total", () => {
  const item = { ...row, usage: { input_tokens: 10, output_tokens: 5, total_tokens: null,
    cached_input_tokens: null, cache_write_input_tokens: null, reasoning_output_tokens: null } };
  const props = { data: { ready: true, loading: false, error: "", items: [item] }, state: {}, onState() {} } as unknown as ObsViewProps;
  const html = renderToStaticMarkup(createElement(OverviewView, props));
  expect(html).toContain("已报告合计 未知");
  expect(html).not.toContain("≈10");
  expect(html).not.toContain("≈5");
});

test("模型请求详情完整显示上报的总量、缓存与推理 token", () => {
  const fixture = detailFixtures[0]!;
  const detail = parseTurnObservations(fixture.body, fixture.body);
  const request = detail.requests[0]!;
  detail.requests = [{ ...request, id: "token-detail", usage: { usage_reports: 2, usage_final: true,
    usage: { input_tokens: 9123, output_tokens: 2173, total_tokens: 11296, cached_input_tokens: 4001,
      cache_write_input_tokens: 0, reasoning_output_tokens: 432 } } }];
  const props = { data: { ready: true, loading: false, error: "", items: [], detail },
    state: { run: detail.run.run_id, session: detail.session_id, turn: detail.turn_id, item: "token-detail", status: "" },
    onState() {}, workspace: "team", dark: false } as unknown as ObsViewProps;
  const html = renderToStaticMarkup(createElement(RunsView, props));
  const document = new DOMParser().parseFromString(html, "text/html");
  const inspector = document.querySelector(".obs-inspector")!;
  const facts = Object.fromEntries([...inspector.querySelectorAll(".obs-fact")].map(fact => [
    fact.querySelector("span")?.textContent, fact.querySelector("strong")?.textContent,
  ]));
  expect(facts).toMatchObject({ "输入 token": "9,123", "输出 token": "2,173", "总 token": "11,296",
    "缓存读取 token": "4,001", "缓存写入 token": "0", "推理输出 token": "432" });
  expect(inspector.textContent).not.toContain("≈");
});

test("empty scope export has only a header, not invented rows", () => {
  const csv = observationCsv("team", []);
  expect(csv.split("\r\n")).toHaveLength(2);
});
