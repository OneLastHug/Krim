// Local browser fixture: the real App and clients, with in-memory API responses.
// It is not imported by the production entrypoint and never calls a model/server API.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "../../src/App";
import "../../src/styles/fonts.css";
import "../../src/index.css";

const query = new URLSearchParams(location.search);
query.set("view", "chat"); query.set("workspace", "local"); query.set("session", "feature-preview");
history.replaceState(null, "", location.pathname + "?" + query);
const session = {
  id: "feature-preview", workspace_id: "local", owner_id: "preview-owner", create_key: "preview",
  title: "会话功能验收", target_key: "demo", target_fingerprint: "a".repeat(64),
  context_policy: { max_bytes: 131072, max_messages: null, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" },
  revision: 1, status: "open", dispatch_paused: false, created_at: 1, closed_at: null,
  archived_at: null, deleted_at: null, lifecycle_revision: 0,
};
const turn = {
  id: "preview-turn", session_id: session.id, workspace_id: "local", owner_id: session.owner_id,
  ordinal: 0, request_key: "preview-turn", text: "检查资料，制定计划，并向我确认接下来的实现方式。",
  accepted_revision: 0, status: "completed", run_id: null, context_fingerprint: null,
  transcript: [{ role: "assistant", content: "资料已经整理好，请确认下面的问题和计划。", call_id: null, tool_calls: [] }],
  code: null, created_at: 1, started_at: 1, finished_at: 2,
};
const requestBase = {
  status: "pending", revision: 1, workspace_id: "local", owner_id: session.owner_id,
  session_id: session.id, run_id: "preview-run", turn_id: turn.id, request_key: "preview-question",
  mode_revision: 1, response: null, created_at: 1, updated_at: 1,
};
const pending: Record<string, unknown>[] = [
  { ...requestBase, id: "preview-question", kind: "user_input", payload: { questions: [
    { id: "scope", prompt: "这次优先完成哪一部分？" + "较长的问题用于检查窄屏换行。".repeat(6), mode: "single", allow_text: true,
      options: [{ id: "core", label: "先完成核心功能", description: "保持现有界面，仅接入新的会话功能。" },
        { id: "tests", label: "先补充测试", description: "检查错误状态、身份切换、分页和问题答复恢复。" }] },
  ] } },
  { ...requestBase, id: "preview-confirmation", kind: "plan_confirmation", payload: {
    plan: "1. 核对当前实现和接口。\n2. 实现选定的范围。\n3. 检查长文字、窄屏和错误状态。\n" + "长路径换行检查：docs/research/" + "long-path-section/".repeat(20),
  } },
];
let mode = "plan", modeRevision = 1;
const compactions = new Map<string, Record<string, unknown>>();
let compactOffline = query.has("compact_offline");
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const realFetch = window.fetch.bind(window);
window.fetch = async (input, options) => {
  const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url, location.origin);
  const path = url.pathname;
  if (!path.startsWith("/v2/") && path !== "/healthz") return realFetch(input, options);
  if (path === "/healthz") return json({ http_enabled: false, product_eval_enabled: false, evaluation_automation_enabled: false });
  if (path === "/v2/me") return json({ principal_id: session.owner_id, generation: 1, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
  if (path === "/v2/me/profile") return json({ error: { code: "account_management_unavailable" } }, 409);
  if (path.startsWith("/v2/targets/")) return json({ error: { code: "not_found" } }, 404);
  const base = "/v2/workspaces/local/sessions";
  if (path === base) return json([session]);
  if (path === base + "/" + session.id) return json(session);
  if (path === base + "/" + session.id + "/turns") return json([turn]);
  if (path.endsWith("/context/compactions")) {
    const body = JSON.parse(String(options?.body ?? "{}"));
    const key = String(body.request_key);
    const existing = compactions.get(key);
    const receipt = existing ?? { request_key: key, session_id: session.id, revision: compactions.size + 1,
      reason: null, status: "pending", created_at: 1, created: true };
    compactions.set(key, receipt);
    if (compactOffline) { compactOffline = false; throw new TypeError("fixture response lost after acceptance"); }
    return json({ ...receipt, created: !existing }, 202);
  }
  if (path.endsWith("/plan")) {
    if (query.has("fixture_error")) return json({ error: { code: "storage_unavailable" } }, 503);
    const all = [
      { id: "a", text: "已完成资料调查", status: "completed" },
      { id: "b", text: "进行中：" + "不可换行的长文字long-text".repeat(24), status: "in_progress" },
      { id: "c", text: "等待用户确认", status: "blocked" },
      { id: "d", text: "最后检查结果", status: "pending" },
    ];
    const offset = Number(url.searchParams.get("offset") ?? 0), limit = 2;
    return json({ version: "planning/1", session_id: session.id, revision: 1, content_hash: "a".repeat(64), updated_at: 1,
      counts: { pending: 1, in_progress: 1, completed: 1, blocked: 1 }, total: 4, matched_total: 4,
      steps: all.slice(offset, offset + limit), offset, limit, next_offset: offset + limit < all.length ? offset + limit : null });
  }
  if (path.endsWith("/interactions")) return json({ mode, revision: modeRevision, pending });
  if (path.includes("/interactions/")) {
    const id = path.split("/").at(-2), index = pending.findIndex(item => item.id === id);
    if (index < 0) return json({ error: { code: "not_found" } }, 404);
    const body = JSON.parse(String(options?.body ?? "{}"));
    const result = { ...pending[index], status: path.endsWith("/cancel") ? "cancelled" : "answered", revision: 2, response: body, updated_at: 2 };
    if (pending[index]?.kind === "plan_confirmation" && body.confirmed) { mode = "execute"; modeRevision += 1; }
    pending.splice(index, 1);
    return json(result);
  }
  return json([]);
};
createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
