import { object } from "../api.ts";
export type Json = null | boolean | number | string | Json[] | {
    [key: string]: Json;
};
export interface Reference {
    expected: Json;
    expected_status: "completed" | "failed";
    expected_code: string | null;
    expected_tools: Json[] | null;
    trajectory: {
        [key: string]: Json;
    } | null;
    expected_schema: {
        [key: string]: Json;
    } | null;
}
export interface CaseSelection {
    case_id: string;
    source_kind: "manual" | "run";
    input?: string;
    selected_input?: string;
    source_run_id?: string;
    candidate_fingerprint?: string;
    reference: Reference;
    reason: string;
}
export interface FreezeBody {
    schema_version: 1;
    name: string;
    version: string;
    cases: CaseSelection[];
}
export interface Candidate {
    source_kind: "run";
    source_run_id: string;
    eligibility: string;
    reasons: string[];
    candidate_fingerprint: string;
    selected_input: string | null;
    input_fingerprint: string;
    [key: string]: unknown;
}
export interface FrozenCase {
    case_id: string;
    input: string;
    input_fingerprint: string;
    source: Candidate | {
        source_kind: "manual";
        [key: string]: unknown;
    };
    reference: Reference;
    reference_fingerprint: string;
    reason: string;
}
export interface Dataset {
    schema_version: 1;
    name: string;
    version: string;
    fingerprint: string;
    created_at: number;
    cases: FrozenCase[];
}
export interface EvalData {
    id: string;
    request_key: string;
    status: string;
    code: string | null;
    created_at: number;
    finished_at: number | null;
    stop_requested: boolean;
    revision: number;
    planned_count: number;
    finished_count: number;
}
export interface Rule {
    name: string;
    version: string;
    config: {
        [key: string]: Json;
    };
}
export interface EvalBody {
    request_key: string;
    dataset: {
        name: string;
        version: string;
        fingerprint: string;
    };
    target_key: string;
    rules: Rule[];
    execution: {
        repetitions: number;
        concurrency: 1;
        item_timeout_seconds: 12;
        scorer_timeout_seconds: 1;
        batch_timeout_seconds: 600;
    };
}
export interface Report {
    eval_id: string;
    revision: number;
    report_hash: string;
    complete: boolean;
    status: string;
    code: string | null;
    dataset: Dataset;
    target_descriptor: Record<string, unknown>;
    target_fingerprint: string;
    items: Record<string, unknown>[];
    summary: Record<string, unknown>;
    [key: string]: unknown;
}
const fp = /^[0-9a-f]{64}$/;
const statuses = ["queued", "running", "completed", "failed", "aborted", "interrupted"];
function requireThat(value: unknown): asserts value {
    if (!value)
        throw new Error("响应结构无效");
}
export function canonical(value: unknown): string {
    if (Array.isArray(value))
        return "[" + value.map(canonical).join(",") + "]";
    if (value !== null && typeof value === "object")
        return "{" + Object.keys(value).sort().map(k => JSON.stringify(k) + ":" + canonical((value as Record<string, unknown>)[k])).join(",") + "}";
    return JSON.stringify(value);
}
export async function hash(value: unknown): Promise<string> { return [...new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonical(value))))].map(x => x.toString(16).padStart(2, "0")).join(""); }
export function strictJson(text: string): Json {
    if (new TextEncoder().encode(text).length > 4096)
        throw new Error("参考内容过长");
    let index = 0;
    function space() {
        while (/[ \t\r\n]/.test(text[index] ?? "") && index < text.length)
            index++;
    }
    function value(depth: number): Json {
        if (depth > 16)
            throw new Error("JSON层数过多");
        space();
        const start = index;
        if (text[index] === '"') {
            index++;
            let escaped = false;
            while (index < text.length) {
                const c = text[index++]!;
                if (c === '"' && !escaped) {
                    const result = JSON.parse(text.slice(start, index)) as string;
                    if (/[\uD800-\uDFFF]/u.test(result))
                        throw new Error("JSON字符无效");
                    return result;
                }
                if (c === '\\' && !escaped)
                    escaped = true;
                else
                    escaped = false;
            }
            throw new Error("JSON字符串未结束");
        }
        if (text[index] === '{' || text[index] === '[') {
            const array = text[index++] === '[';
            const result: Json[] | Record<string, Json> = array ? [] : {};
            const keys = new Set<string>();
            space();
            if (text[index] === (array ? ']' : '}')) {
                index++;
                return result;
            }
            while (true) {
                if (array)
                    (result as Json[]).push(value(depth + 1));
                else {
                    space();
                    if (text[index] !== '"')
                        throw new Error("JSON键无效");
                    const key = value(depth + 1);
                    if (typeof key !== "string" || keys.has(key))
                        throw new Error("JSON重复键");
                    keys.add(key);
                    space();
                    if (text[index++] !== ':')
                        throw new Error("JSON键值无效");
                    Object.defineProperty(result, key, { value: value(depth + 1), enumerable: true, configurable: true, writable: true });
                }
                space();
                const c = text[index++];
                if (c === (array ? ']' : '}'))
                    return result;
                if (c !== ',')
                    throw new Error("JSON分隔无效");
            }
        }
        const token = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(index))?.[0];
        if (!token)
            throw new Error("JSON值无效");
        index += token.length;
        const result = JSON.parse(token) as Json;
        if (typeof result === "number" && (!Number.isFinite(result) || Number.isInteger(result) && Math.abs(result) > Number.MAX_SAFE_INTEGER))
            throw new Error("JSON数值超限");
        return result;
    }
    const result = value(0);
    space();
    if (index !== text.length)
        throw new Error("JSON尾部无效");
    return result;
}
export function parseCandidate(value: unknown): Candidate { const r = object(value); requireThat(r.schema_version === 1 && r.source_kind === "run" && typeof r.source_run_id === "string" && ["eligible", "needs_context", "ineligible"].includes(String(r.eligibility)) && Array.isArray(r.reasons) && r.reasons.every(v => typeof v === "string") && fp.test(String(r.candidate_fingerprint)) && (r.selected_input === null || typeof r.selected_input === "string") && fp.test(String(r.input_fingerprint))); return r as Candidate; }
export function parseReference(value: unknown): Reference { const r = object(value); const keys = ["expected", "expected_status", "expected_code", "expected_tools", "trajectory", "expected_schema"]; requireThat(Object.keys(r).length === keys.length && keys.every(k => k in r) && ["completed", "failed"].includes(String(r.expected_status)) && (r.expected_code === null || typeof r.expected_code === "string") && (r.expected_status === "completed" ? r.expected_code === null : typeof r.expected_code === "string") && (r.expected_tools === null || Array.isArray(r.expected_tools)) && (r.trajectory === null || typeof r.trajectory === "object" && !Array.isArray(r.trajectory)) && (r.expected_schema === null || typeof r.expected_schema === "object" && !Array.isArray(r.expected_schema))); strictJson(JSON.stringify(r.expected)); return r as unknown as Reference; }
const configs: Record<string, Record<string, Json>> = { status_code: { match_code: true }, exact_match: { normalize_whitespace: false }, json_match: { strict_json: true }, json_schema: { schema_source: "reference", remote_refs: false }, tool_calls: { ordered: true }, trajectory: { scope: "single_round" } };
export function parseRule(value: unknown): Rule { const r = object(value); requireThat(Object.keys(r).length === 3 && typeof r.name === "string" && r.name in configs && r.version === "1" && canonical(r.config) === canonical(configs[r.name])); return r as unknown as Rule; }
export function parseDataset(value: unknown): Dataset {
    const r = object(value);
    requireThat(r.schema_version === 1 && typeof r.name === "string" && typeof r.version === "string" && fp.test(String(r.fingerprint)) && typeof r.created_at === "number" && Number.isFinite(r.created_at) && Array.isArray(r.cases) && r.cases.length > 0 && r.cases.length <= 8);
    const ids = new Set<string>();
    for (const v of r.cases) {
        const c = object(v), s = object(c.source), ref = object(c.reference);
        requireThat(typeof c.case_id === "string" && !ids.has(c.case_id) && typeof c.input === "string" && typeof c.reason === "string" && fp.test(String(c.input_fingerprint)) && fp.test(String(c.reference_fingerprint)) && ["manual", "run"].includes(String(s.source_kind)) && s.eligibility === "eligible" && "expected" in ref && ["completed", "failed"].includes(String(ref.expected_status)));
        ids.add(c.case_id);
    }
    return r as unknown as Dataset;
}
export function parseEval(value: unknown): EvalData {
    const r = object(value);
    const keys = ["id", "request_key", "status", "code", "created_at", "finished_at", "stop_requested", "revision", "planned_count", "finished_count"];
    requireThat(Object.keys(r).length === keys.length && keys.every(k => k in r) && typeof r.id === "string" && typeof r.request_key === "string" && statuses.includes(String(r.status)) && (r.code === null || typeof r.code === "string") && typeof r.created_at === "number" && Number.isFinite(r.created_at) && (r.finished_at === null || typeof r.finished_at === "number" && Number.isFinite(r.finished_at)) && typeof r.stop_requested === "boolean");
    for (const key of ["revision", "planned_count", "finished_count"])
        requireThat(typeof r[key] === "number" && Number.isInteger(r[key]) && r[key] >= 0);
    requireThat(Number(r.revision) >= 1 && Number(r.planned_count) >= 1 && Number(r.planned_count) <= 24 && Number(r.finished_count) <= Number(r.planned_count));
    return r as unknown as EvalData;
}
export function parseReport(value: unknown): Report {
    const r = object(value);
    requireThat(r.schema_version === 1 && typeof r.eval_id === "string" && Number.isInteger(r.revision) && typeof r.complete === "boolean" && statuses.includes(String(r.status)) && fp.test(String(r.report_hash)) && fp.test(String(r.target_fingerprint)) && Array.isArray(r.items) && r.items.length > 0 && r.items.length <= 24 && Array.isArray(r.rules) && r.rules.length > 0 && r.rules.length <= 6);
    parseDataset(r.dataset);
    object(r.target_descriptor);
    r.rules.forEach(parseRule);
    const summary = object(r.summary), n = r.items.length, m = r.rules.length;
    let scored = 0, total = 0, passed = 0, completeItems = 0;
    // 只核对报告计数与所携带单元对应；不在浏览器执行规则或改写质量。
    for (const raw of r.items) {
        const item = object(raw);
        requireThat(typeof item.case_id === "string" && Number.isInteger(item.repetition));
        if (item.quality === "passed")
            passed++;
        if (item.scores !== null) {
            requireThat(Array.isArray(item.scores) && item.scores.length === m);
            let complete = true;
            for (const rawScore of item.scores) {
                const score = object(rawScore);
                requireThat(["scored", "skipped", "error"].includes(String(score.status)));
                if (score.status === "scored") {
                    requireThat(typeof score.value === "number" && (score.value === 0 || score.value === 1));
                    scored++;
                    total += score.value;
                }
                else {
                    requireThat(score.value === null);
                    complete = false;
                }
            }
            if (complete)
                completeItems++;
        }
    }
    requireThat(r.planned_count === n && summary.planned === n && summary.planned_cells === n * m && summary.scored_cells === scored && summary.passed === passed && summary.pass_rate === passed / n && summary.cell_coverage === scored / (n * m) && summary.item_coverage === completeItems / n && summary.mean === (scored ? total / scored : null));
    return r as unknown as Report;
}
export function revisionSelection(dataset: Dataset): FreezeBody { return { schema_version: 1, name: dataset.name, version: dataset.version, cases: dataset.cases.map(c => ({ case_id: c.case_id, source_kind: c.source.source_kind, ...(c.source.source_kind === "manual" ? { input: c.input } : { selected_input: c.input, source_run_id: c.source.source_run_id, candidate_fingerprint: c.source.candidate_fingerprint }), reference: c.reference, reason: c.reason } as CaseSelection)) }; }
export async function matchesRevision(dataset: Dataset, request: FreezeBody): Promise<boolean> {
    const normalized = { ...request, cases: [...request.cases].sort((a, b) => a.case_id < b.case_id ? -1 : a.case_id > b.case_id ? 1 : 0) };
    if (canonical(revisionSelection(dataset)) !== canonical(normalized))
        return false;
    for (const c of dataset.cases) {
        const selected = request.cases.find(v => v.case_id === c.case_id)!;
        // 服务端DatasetRevision校验其持久参考指纹；JS不重算Python浮点JSON的字节hash。
        // 上面的完整原提交比较仍包含参考、理由、来源及其身份；字符串输入另核对SHA256。
        if (await hash(selected.input ?? selected.selected_input) !== c.input_fingerprint)
            return false;
    }
    return true;
}
