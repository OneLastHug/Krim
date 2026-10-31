import { RequestError, object, request } from "../api.ts";
import { canonical, matchesRevision, parseDataset, parseEval, type Dataset, type EvalBody, type EvalData, type FreezeBody } from "./eval-contracts.ts";
export const FREEZE_INTENT = "krim-agent-freeze-intent-v1", EVAL_INTENT = "krim-agent-eval-intent-v1", EVAL_CURRENT = "krim-agent-eval-v1";
export interface FreezeIntent {
    body: FreezeBody;
    draftId: string;
    editRevision: number;
    nonce: string;
}
export function unadmitted(error: unknown, kind: "freeze" | "eval", method: "GET" | "POST"): boolean {
    if (method !== "POST" || !(error instanceof RequestError))
        return false;
    if (error.status === 401 && error.code === "unauthorized")
        return true;
    if (error.status === 422 && ["invalid_input", "input_limit"].includes(error.code))
        return true;
    if (kind === "eval")
        return error.status === 429 && error.code === "capacity";
    return error.status === 409 && ["source_not_eligible", "source_changed", "candidate_input_mismatch"].includes(error.code);
}
export function loadFreeze(store: Storage): FreezeIntent | null {
    const raw = store.getItem(FREEZE_INTENT);
    if (!raw)
        return null;
    const value = object(JSON.parse(raw));
    if (typeof value.draftId !== "string" || typeof value.nonce !== "string" || !Number.isInteger(value.editRevision))
        throw new Error("原冻结请求无效");
    const body = object(value.body);
    if (typeof body.name !== "string" || typeof body.version !== "string" || !Array.isArray(body.cases))
        throw new Error("原冻结请求无效");
    return value as unknown as FreezeIntent;
}
function clearFreeze(store: Storage, intent: FreezeIntent) {
    if (loadFreeze(store)?.nonce === intent.nonce)
        store.removeItem(FREEZE_INTENT);
}
export async function freezeOrFind(store: Storage, fresh: FreezeIntent, key: string, current = () => true): Promise<{
    dataset: Dataset;
    intent: FreezeIntent;
}> {
    if (!current()) throw new Error("request_scope_changed");
    const prior = loadFreeze(store), intent = prior ?? fresh;
    if (!prior)
        store.setItem(FREEZE_INTENT, JSON.stringify(intent));
    const path = `/v1/datasets/${encodeURIComponent(intent.body.name)}/${encodeURIComponent(intent.body.version)}`;
    let dataset: Dataset;
    try {
        dataset = parseDataset(await request(prior ? path : "/v1/datasets", key, prior ? undefined : intent.body));
    }
    catch (error) {
        if (!current()) throw new Error("request_scope_changed");
        if (!prior && unadmitted(error, "freeze", "POST")) {
            clearFreeze(store, intent);
            throw error;
        }
        if (!prior && error instanceof RequestError && error.status === 409 && error.code === "revision_conflict") {
            const existing = parseDataset(await request(path, key));
            if (existing.name !== intent.body.name || existing.version !== intent.body.version)
                throw new Error("修订身份不一致");
            if (!current()) throw new Error("request_scope_changed");
            // 原POST已由服务端规范指纹判为冲突，GET再确认原键确有修订。
            // JS数值表示合并（如1与1.0）不能反过来把这个冲突升级为成功。
            clearFreeze(store, intent);
            throw new Error("修订内容冲突，请使用新版本");
        }
        else
            dataset = parseDataset(await request(path, key));
    }
    if (!(await matchesRevision(dataset, intent.body)))
        throw new Error("修订内容不一致");
    if (!current()) throw new Error("request_scope_changed");
    clearFreeze(store, intent);
    return { dataset, intent };
}
export function loadEval(store: Storage): EvalBody | null {
    const raw = store.getItem(EVAL_INTENT);
    if (!raw)
        return null;
    const value = object(JSON.parse(raw));
    if (typeof value.request_key !== "string" || typeof value.target_key !== "string" || !Array.isArray(value.rules) || typeof value.dataset !== "object")
        throw new Error("原评测请求无效");
    return value as unknown as EvalBody;
}
export async function evalOrFind(store: Storage, body: EvalBody, key: string, current = () => true): Promise<EvalData> {
    if (!current()) throw new Error("request_scope_changed");
    const prior = loadEval(store), intent = prior ?? body;
    if (!prior)
        store.setItem(EVAL_INTENT, JSON.stringify(intent));
    const path = "/v1/evals/by-key/" + encodeURIComponent(intent.request_key);
    let view: EvalData;
    try {
        view = parseEval(await request(prior ? path : "/v1/evals", key, prior ? undefined : intent));
    }
    catch (error) {
        if (!current()) throw new Error("request_scope_changed");
        if (!prior && unadmitted(error, "eval", "POST")) {
            if (loadEval(store)?.request_key === intent.request_key)
                store.removeItem(EVAL_INTENT);
            throw error;
        }
        view = parseEval(await request(path, key));
    }
    if (!current()) throw new Error("request_scope_changed");
    const confirmed = parseEval(await request(path, key));
    if (confirmed.id !== view.id || confirmed.request_key !== intent.request_key)
        throw new Error("评测身份不一致");
    return confirmed;
}
export function acknowledgeEval(store: Storage, body: EvalBody, view: EvalData): void {
    // 只有调用方已接收并持久保存原身份，才结束这份完整请求的待确认状态。
    if (view.request_key !== body.request_key || store.getItem(EVAL_CURRENT) !== view.id || !sameRequest(loadEval(store), body))
        throw new Error("评测确认身份不一致");
    store.removeItem(EVAL_INTENT);
}
export function sameRequest(a: unknown, b: unknown) { return canonical(a) === canonical(b); }
