import { object, parseRun, request, isUnadmittedCreate } from "../api";
export const INTENT = "krim-agent-intent", CURRENT = "krim-agent-run";
export interface Intent {
    text: string;
    request_key: string;
    target: "demo";
}
export function savedIntent(store: Storage): Intent | null {
    const raw = store.getItem(INTENT);
    if (!raw)
        return null;
    const body = object(JSON.parse(raw));
    if (typeof body.text !== "string" || typeof body.request_key !== "string" || body.target !== "demo")
        throw new Error("invalid_record");
    return { text: body.text, request_key: body.request_key, target: "demo" };
}
export async function createOrFind(store: Storage, text: string, key: string, current: () => boolean) {
    const saved = savedIntent(store);
    const body: Intent = saved ?? { text, request_key: crypto.randomUUID(), target: "demo" };
    if (saved && saved.text !== text)
        throw new Error("request_conflict");
    store.setItem(INTENT, JSON.stringify(body));
    const path = "/v1/runs/by-key/" + encodeURIComponent(body.request_key);
    let run;
    try {
        run = parseRun(await request(saved ? path : "/v1/runs", key, saved ? undefined : body));
    }
    catch (error) {
        if (!saved && isUnadmittedCreate(error)) {
            if (savedIntent(store)?.request_key === body.request_key)
                store.removeItem(INTENT);
            throw error;
        }
        run = parseRun(await request(path, key));
    }
    const confirmed = parseRun(await request(path, key));
    if (confirmed.id !== run.id || confirmed.request_key !== body.request_key)
        throw new Error("identity");
    if (current() && savedIntent(store)?.request_key === body.request_key)
        store.removeItem(INTENT);
    return confirmed;
}
export async function replyApproval(store: Storage, id: string, approval: Record<string, unknown>, allow: boolean, key: string) {
    if (approval.run_id !== id)
        throw new Error("identity");
    const storageKey = "krim-agent-approval-" + String(approval.request_id);
    const prior = store.getItem(storageKey);
    const body = prior ? object(JSON.parse(prior)) : { request_key: crypto.randomUUID(), call_id: approval.call_id, arguments_hash: approval.arguments_hash, tool_hash: approval.tool_hash, allow };
    if (body.allow !== allow || ["call_id", "arguments_hash", "tool_hash"].some(field => body[field] !== approval[field]))
        throw new Error("approval_conflict");
    store.setItem(storageKey, JSON.stringify(body));
    return request(`/v1/runs/${encodeURIComponent(id)}/approvals/${encodeURIComponent(String(approval.request_id))}`, key, body);
}
export function initialIntent() { try {
    return { intent: savedIntent(localStorage), invalid: false };
}
catch {
    return { intent: null, invalid: true };
} }
