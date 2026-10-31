import { useEffect, useRef, useState } from "react";
import { object, request } from "../api";
import { reason } from "../lib/copy";
import { parseCandidate, parseDataset, strictJson, type Candidate, type CaseSelection, type Dataset, type Json, type Reference } from "../lib/eval-contracts";
import { FREEZE_INTENT, freezeOrFind, loadFreeze, type FreezeIntent } from "../lib/eval-requests";
export interface Draft {
    caseId: string;
    input: string;
    expected: string;
    reason: string;
    status: "completed" | "failed";
    code: string;
    tools: string;
    trajectory: string;
    schema: string;
    sourceRun: string;
}
const blank = (): Draft => ({ caseId: "case-1", input: "", expected: "", reason: "", status: "completed", code: "", tools: "", trajectory: "", schema: "", sourceRun: "" });
function jsonObject(text: string): {
    [key: string]: Json;
} | null {
    if (!text.trim())
        return null;
    return object(strictJson(text)) as {
        [key: string]: Json;
    };
}
function reference(draft: Draft): Reference {
    if (!draft.expected.trim())
        throw new Error("请填写预期结果");
    let tools: Json[] | null = null;
    if (draft.tools.trim()) {
        const parsed = strictJson(draft.tools);
        if (!Array.isArray(parsed))
            throw new Error("工具序列应为JSON数组");
        tools = parsed.map(raw => { const t = object(raw); return { name: t.name as Json, arguments: (t.arguments ?? null) as Json, executor_started: (t.executor_started ?? null) as Json, status: (t.status ?? null) as Json }; });
    }
    const raw = jsonObject(draft.trajectory);
    const trajectory = raw ? { exact_steps: raw.exact_steps ?? null, min_steps: raw.min_steps ?? null, max_steps: raw.max_steps ?? null, tool_names: raw.tool_names ?? null } : null;
    return { expected: strictJson(draft.expected), expected_status: draft.status, expected_code: draft.code.trim() || null, expected_tools: tools, trajectory, expected_schema: jsonObject(draft.schema) };
}
export function useDataset(key: string, sourceRun: string, active: boolean) {
    const [initial] = useState(() => {
        try {
            return { intent: loadFreeze(localStorage), invalid: false };
        }
        catch {
            return { intent: null, invalid: true };
        }
    });
    const [draft, setDraft] = useState<Draft>(blank), [name, setName] = useState(initial.intent?.body.name ?? ""), [version, setVersion] = useState(initial.intent?.body.version ?? "1");
    const [cases, setCases] = useState<CaseSelection[]>(initial.intent?.body.cases ?? []), [candidate, setCandidate] = useState<Candidate | null>(null), [source, setSource] = useState<Candidate | null>(null);
    const [frozen, setFrozen] = useState<Dataset | null>(null), [pending, setPending] = useState(Boolean(initial.intent) || initial.invalid), [busy, setBusy] = useState(false), [notice, setNotice] = useState(initial.invalid ? "原冻结请求记录无效" : "");
    const draftId = useRef(initial.intent?.draftId ?? crypto.randomUUID()), edit = useRef(initial.intent?.editRevision ?? 0), epoch = useRef(0), reader = useRef<AbortController | null>(null), mounted = useRef(false), flight = useRef(false), keyRef = useRef(key);
    const activeRef = useRef(active);
    keyRef.current = key; activeRef.current = active;
    function available() { return mounted.current && activeRef.current && keyRef.current === key; }
    function invalidate() { epoch.current++; reader.current?.abort(); }
    useEffect(() => { mounted.current = true; return () => { mounted.current = false; invalidate(); }; }, []);
    useEffect(() => { invalidate(); setCandidate(null); }, [key, active]);
    function changed() { edit.current++; invalidate(); setCandidate(null); setNotice(""); }
    function change(patch: Partial<Draft>) { changed(); setDraft(d => ({ ...d, ...patch })); }
    function rename(value: string) { changed(); setName(value); }
    function revise(value: string) { changed(); setVersion(value); }
    async function collect(id: string, includeInput = false) {
        if (!available()) return;
        invalidate();
        const token = epoch.current, revision = edit.current, controller = new AbortController();
        reader.current = controller;
        try {
            const value = parseCandidate(await request(`/v1/runs/${encodeURIComponent(id)}/candidate?include_input=${includeInput}`, key, undefined, controller.signal));
            if (!available() || token !== epoch.current || revision !== edit.current)
                return;
            if (value.source_run_id !== id)
                throw new Error("来源身份不一致");
            setCandidate(value);
            if (includeInput) {
                if (value.eligibility !== "eligible" || value.selected_input === null)
                    throw new Error("此来源不能选入");
                edit.current++;
                setSource(value);
                setDraft(d => ({ ...d, input: value.selected_input!, sourceRun: id }));
            }
        }
        catch (error) {
            if (available() && token === epoch.current)
                setNotice(reason(error, error instanceof Error ? error.message : "来源读取未完成"));
        }
    }
    useEffect(() => {
        if (active && sourceRun) {
            changed();
            setDraft(d => ({ ...d, sourceRun }));
            void collect(sourceRun);
        }
    }, [sourceRun, active]);
    function manual() { changed(); setSource(null); }
    function add() {
        try {
            if (!draft.caseId.trim() || !draft.reason.trim() || !draft.input.trim())
                throw new Error("请填写样本、输入和理由");
            const ref = reference(draft);
            const selected: CaseSelection = { case_id: draft.caseId, source_kind: source ? "run" : "manual", ...(source ? { source_run_id: source.source_run_id, candidate_fingerprint: source.candidate_fingerprint, selected_input: draft.input } : { input: draft.input }), reference: ref, reason: draft.reason };
            if (cases.length >= 8 && !cases.some(c => c.case_id === selected.case_id))
                throw new Error("最多8个样本");
            changed();
            setCases(old => [...old.filter(c => c.case_id !== selected.case_id), selected].sort((a, b) => a.case_id < b.case_id ? -1 : 1));
        }
        catch (error) {
            setNotice(error instanceof Error ? error.message : "样本内容无效");
        }
    }
    function remove(id: string) { changed(); setCases(old => old.filter(c => c.case_id !== id)); }
    function editCase(value: CaseSelection) { changed(); const r = value.reference; setSource(value.source_kind === "run" ? { source_kind: "run", source_run_id: value.source_run_id!, candidate_fingerprint: value.candidate_fingerprint!, selected_input: value.selected_input!, eligibility: "eligible", reasons: ["eligible"], input_fingerprint: "" } : null); setDraft({ caseId: value.case_id, input: value.input ?? value.selected_input ?? "", expected: JSON.stringify(r.expected), reason: value.reason, status: r.expected_status, code: r.expected_code ?? "", tools: r.expected_tools === null ? "" : JSON.stringify(r.expected_tools), trajectory: r.trajectory === null ? "" : JSON.stringify(r.trajectory), schema: r.expected_schema === null ? "" : JSON.stringify(r.expected_schema), sourceRun: value.source_run_id ?? "" }); }
    async function freeze() {
        if (!available() || flight.current)
            return;
        flight.current = true;
        setBusy(true);
        setNotice("");
        const capturedKey = key;
        const fresh: FreezeIntent = { body: { schema_version: 1, name, version, cases }, draftId: draftId.current, editRevision: edit.current, nonce: crypto.randomUUID() };
        try {
            const result = await freezeOrFind(localStorage, fresh, key, () => available() && keyRef.current === capturedKey);
            if (available() && keyRef.current === capturedKey) {
                if (result.intent.draftId === draftId.current && result.intent.editRevision === edit.current)
                    setFrozen(result.dataset);
                setNotice(result.intent.editRevision === edit.current ? "修订已冻结" : "原提交已确认");
            }
        }
        catch (error) {
            if (available() && keyRef.current === capturedKey)
                setNotice(localStorage.getItem(FREEZE_INTENT) ? "冻结结果未确认" : reason(error, error instanceof Error ? error.message : "冻结未完成"));
        }
        finally {
            flight.current = false;
            if (mounted.current) {
                setPending(localStorage.getItem(FREEZE_INTENT) !== null);
                setBusy(false);
            }
        }
    }
    async function open() {
        if (!available()) return;
        invalidate();
        const token = epoch.current;
        const controller = new AbortController();
        reader.current = controller;
        setNotice("");
        try {
            const value = parseDataset(await request(`/v1/datasets/${encodeURIComponent(name)}/${encodeURIComponent(version)}`, key, undefined, controller.signal));
            if (available() && token === epoch.current) {
                if (value.name !== name || value.version !== version)
                    throw new Error("修订身份不一致");
                setFrozen(value);
            }
        }
        catch (error) {
            if (available() && token === epoch.current)
                setNotice(reason(error, "修订读取未完成"));
        }
    }
    return { draft, name, version, cases, candidate, source, frozen, pending, busy, notice, change, rename, revise, collect, manual, add, remove, editCase, freeze, open };
}
export type DatasetState = ReturnType<typeof useDataset>;
