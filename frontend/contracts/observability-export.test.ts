import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, test } from "vitest";
import { OverviewView } from "../src/observability/overview-view";
import { SessionView } from "../src/observability/session-view";
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

test("empty scope export has only a header, not invented rows", () => {
  const csv = observationCsv("team", []);
  expect(csv.split("\r\n")).toHaveLength(2);
});
