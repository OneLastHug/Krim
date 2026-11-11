import pageFixture from "./observability-api-page-fixture.json";
import detailFixtures from "./observability-api-detail-fixtures.json";

/** Shared host-navigation fixture, projected from the isolated API's real shape. */
export function navigationObservation(sessionId: string, turnId: string, runId: string, title: string) {
  const item = { ...structuredClone(pageFixture.items[0]!), session_id: sessionId, turn_id: turnId,
    run_id: runId, trace_id: `trace-${runId}`, session_title: title };
  const detail = structuredClone(detailFixtures[0]!.body);
  detail.session_id = sessionId;
  detail.turn_id = turnId;
  detail.run.run_id = runId;
  detail.run.trace_id = item.trace_id;
  detail.trace.run_id = runId;
  detail.trace.trace_id = item.trace_id;
  for (const span of detail.trace.spans) {
    span.run_id = runId;
    span.trace_id = item.trace_id;
  }
  return { item, detail };
}

export function navigationObservationPage(workspace: string, items: unknown[]) {
  return { scope: { ...pageFixture.scope, workspace_id: workspace }, items, next_cursor: null };
}
