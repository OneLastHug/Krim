import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChatComposer } from "../src/components/ChatComposer";
import { ChatHistory } from "../src/components/ChatHistory";
import { useSession, type SessionState } from "../src/hooks/use-session";
import { CURRENT, INTENTS } from "../src/lib/session-requests";
import type { Session, Turn } from "../src/lib/session-contracts";

const workspace = "local";
const base = `/v2/workspaces/${workspace}/sessions`;
const actor = { principal_id: "owner", generation: 1, legacy: true, workspaces: [{ workspace_id: workspace, role: "admin", approve: true }] };
function session(id: string): Session {
  return { id, workspace_id: workspace, owner_id: actor.principal_id, create_key: `create-${id}`, title: id, target_key: "demo", target_fingerprint: "a".repeat(64), context_policy: { max_bytes: 32768, max_messages: 100, reserved_output_tokens: 100, counter_version: "approx-utf8/1" }, revision: 1, status: "open", dispatch_paused: false, created_at: 1, closed_at: null, archived_at: null, deleted_at: null, lifecycle_revision: 0 };
}
function turn(id: string, sessionId: string, text = id): Turn {
  return { id, session_id: sessionId, workspace_id: workspace, owner_id: actor.principal_id, ordinal: 1, request_key: id, text, accepted_revision: 1, status: "completed", run_id: `run-${id}`, context_fingerprint: null, transcript: [{ role: "assistant", content: `answer: ${text}`, call_id: null, tool_calls: [] }], code: null, created_at: 1, started_at: 1, finished_at: 2 };
}
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
const json = (value: unknown) => new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });

let root: Root, host: HTMLDivElement, chat: SessionState;
let sessions: Session[], turns: Record<string, Turn[]>;
let postGate: ReturnType<typeof deferred> | null;
let reloadGate: ReturnType<typeof deferred> | null;
let receipt: Turn | null;
let unknownPost: boolean;
let postCount: number;
let postRevisions: number[];
const fence = { current: 0 };
const onLegacy = () => {};
function Harness() {
  chat = useSession("", true, fence, onLegacy);
  return <><ChatHistory chat={chat} /><ChatComposer chat={chat} /></>;
}
async function flush() { await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
async function mount(id = "existing") {
  window.history.replaceState(null, "", `/?view=chat&workspace=${workspace}${id ? `&session=${id}` : ""}`);
  await act(async () => { root.render(<Harness />); });
  await flush();
}
async function send(text: string) {
  await act(async () => { chat.edit(text); });
  let sending!: Promise<void>;
  await act(async () => { sending = chat.submit(); });
  return { finish: async () => { await act(async () => { postGate?.resolve(); await sending; }); } };
}
function expectRetained(id: string, text?: string) {
  expect(chat.id).toBe(id);
  expect(chat.session?.id).toBe(id);
  expect(host.textContent).not.toContain("选择或新建对话");
  if (text) expect(host.querySelector(".chat-user-text")?.textContent).toBe(text);
}

beforeEach(() => {
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  localStorage.clear();
  sessions = [session("existing"), session("other")];
  turns = { existing: [turn("old-turn", "existing", "old question")], other: [turn("other-turn", "other", "other question")] };
  postGate = null; reloadGate = null; receipt = null; unknownPost = false; postCount = 0; postRevisions = [];
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, options?: RequestInit) => {
    const url = new URL(String(input), location.origin);
    const path = url.pathname;
    if (path === "/v2/me") return json(actor);
    if (options?.method === "POST" && path === base) {
      const body = JSON.parse(String(options.body));
      const created = { ...session("created"), create_key: body.request_key, title: body.title };
      sessions.push(created); turns.created = [];
      return json(created);
    }
    if (options?.method === "POST" && path.endsWith("/turns")) {
      postCount++;
      const id = path.split("/").at(-2)!;
      const body = JSON.parse(String(options.body));
      postRevisions.push(body.expected_revision);
      receipt = { ...turn(`turn-${postCount}`, id, body.text), request_key: body.request_key, accepted_revision: body.expected_revision };
      await postGate?.promise;
      turns[id]!.push(receipt);
      sessions.find(value => value.id === id)!.revision++;
      if (unknownPost) throw new TypeError("response lost");
      return json(receipt);
    }
    if (path.includes("/turns/by-key/")) return json(receipt);
    if (path === `/v2/workspaces/${workspace}/session-sidebar`) return json({ revision: 0, groups: [], sessions: [] });
    if (path === base) { await reloadGate?.promise; return json(sessions); }
    const selected = sessions.find(value => path === `${base}/${value.id}`);
    if (selected) return json(selected);
    const id = sessions.find(value => path === `${base}/${value.id}/turns`)?.id;
    if (id) { await reloadGate?.promise; return json(turns[id]); }
    throw new Error(`Unexpected request: ${options?.method} ${url}`);
  }));
});
afterEach(async () => {
  postGate?.resolve(); reloadGate?.resolve();
  await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals();
});

describe("current-session turn receipt refresh", () => {
  it("retains the selected session and transcript while the successful receipt reload is delayed", async () => {
    await mount(); expectRetained("existing", "old question");
    postGate = deferred(); reloadGate = deferred();
    const sending = await send("next question");
    expect(chat.sending).toBe(true);
    expect(chat.turnPending?.body.text).toBe("next question");
    expectRetained("existing", "old question");
    await sending.finish();
    expect(chat.reading).toBe(true);
    expect(chat.sending).toBe(false);
    expect(chat.turnPending).toBeUndefined();
    expect(chat.draft).toBe("");
    expectRetained("existing", "old question");
    expect(host.querySelector<HTMLTextAreaElement>("#chat-prompt")?.disabled).toBe(true);
    await act(async () => { chat.edit("wait for latest revision"); });
    await act(async () => { await chat.submit(); });
    expect(postCount).toBe(1);
    expect(new URL(location.href).searchParams.get("turn")).toBe(receipt!.id);
    expect(JSON.parse(localStorage.getItem(CURRENT)!)).toMatchObject({ session_id: "existing", turn_id: receipt!.id });
    reloadGate.resolve(); await flush();
    expect(chat.reading).toBe(false);
    expect(host.querySelector<HTMLTextAreaElement>("#chat-prompt")?.disabled).toBe(false);
    expect(host.querySelectorAll(".chat-user-text")).toHaveLength(2);
    expect(host.textContent).toContain("answer: next question");
  });

  it("retains a newly created empty session when its first message is accepted", async () => {
    await mount("");
    await act(async () => { await chat.create("new conversation", "demo"); });
    await flush(); expectRetained("created");
    expect(host.textContent).toContain("发送第一条消息");
    postGate = deferred(); reloadGate = deferred();
    const sending = await send("first question"); await sending.finish();
    expectRetained("created");
    reloadGate.resolve(); await flush();
    expect(host.textContent).toContain("answer: first question");
    expect(host.textContent).not.toContain("发送第一条消息");
  });

  it("never resets the transcript for consecutive messages", async () => {
    await mount();
    for (let count = 1; count <= 3; count++) {
      postGate = deferred(); reloadGate = deferred();
      const sending = await send(`message ${count}`); await sending.finish();
      expectRetained("existing", "old question");
      expect(host.querySelectorAll(".chat-turn")).toHaveLength(count);
      reloadGate.resolve(); await flush();
      expect(host.querySelectorAll(".chat-turn")).toHaveLength(count + 1);
    }
    expect(postCount).toBe(3);
    expect(postRevisions).toEqual([1, 2, 3]);
  });

  it("keeps an unknown POST pending and recovers its original receipt without clearing history", async () => {
    await mount(); unknownPost = true;
    postGate = deferred(); reloadGate = deferred();
    const sending = await send("recover question"); await sending.finish();
    expectRetained("existing", "old question");
    expect(chat.turnPending?.body.text).toBe("recover question");
    expect(host.querySelector<HTMLTextAreaElement>("#chat-prompt")?.disabled).toBe(true);
    await act(async () => { await chat.recover(chat.turnPending!); });
    expectRetained("existing", "old question");
    expect(chat.turnPending).toBeUndefined();
    expect(JSON.parse(localStorage.getItem(INTENTS)!)).toEqual([]);
    reloadGate.resolve(); await flush();
    expect(host.textContent).toContain("answer: recover question");
    expect(postCount).toBe(1);
  });

  it("still clears the previous session immediately when switching to another session", async () => {
    await mount(); reloadGate = deferred();
    await act(async () => { chat.select(workspace, "other"); });
    expect(chat.session).toBeNull(); expect(chat.history).toEqual([]);
    expect(host.textContent).not.toContain("old question");
    reloadGate.resolve(); await flush();
    expectRetained("other", "other question");
    expect(host.textContent).not.toContain("old question");
  });

  it("reloads the selected session and accepted messages after an explicit refresh", async () => {
    await mount();
    const sending = await send("before refresh"); await sending.finish(); await flush();
    await act(async () => { chat.refresh(); }); await flush();
    expectRetained("existing", "old question");
    expect(host.textContent).toContain("answer: before refresh");
    expect(chat.reading).toBe(false);
  });
});
