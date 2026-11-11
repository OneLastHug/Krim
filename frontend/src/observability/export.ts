import type { ObservationsIndexItem } from "./obs-contracts";

/** Export only the already-authorized current page; do not fetch or include bodies. */
export function observationCsv(workspace: string, items: readonly ObservationsIndexItem[]): string {
  const cell = (value: unknown) => {
    const raw = value === null || value === undefined ? "" : String(value);
    // Spreadsheet formulas are data here, not commands.
    const safe = /^[=+\-@\t\r]/.test(raw) ? `'${raw}` : raw;
    return `"${safe.replaceAll('"', '""')}"`;
  };
  const columns = ["workspace_id", "scope", "session_id", "turn_id", "run_id", "session_title",
    "status", "created_at_utc", "model_requests", "tool_calls", "input_tokens", "output_tokens",
    "total_tokens", "usage_complete", "duration_ms", "cost_usd", "display_currency", "cost_status",
    "original_currency", "price_source", "exchange_rate_source"];
  const rows = items.map(item => [workspace, "returned_page", item.session_id, item.turn_id, item.run_id,
    item.session_title, item.status, new Date(item.created_at * 1000).toISOString(),
    item.model_requests, item.tool_calls, item.usage?.input_tokens, item.usage?.output_tokens,
    item.usage?.total_tokens, item.usage_complete, item.duration_ms,
    null, "USD", item.cost_status, null, null, null]);
  return "﻿" + [columns, ...rows].map(row => row.map(cell).join(",")).join("\r\n") + "\r\n";
}

export function downloadObservationCsv(workspace: string, items: readonly ObservationsIndexItem[]) {
  const blob = new Blob([observationCsv(workspace, items)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `krim-observations-${workspace}-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
