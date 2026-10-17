import { useEffect, useRef, useState } from "react";
import { object, request, RequestError } from "../api";
import { reason } from "../lib/copy";
import { parseEval, parseReport, type Dataset, type EvalBody, type EvalData, type Report, type Rule } from "../lib/eval-contracts";
import { EVAL_CURRENT, EVAL_INTENT, evalOrFind, loadEval } from "../lib/eval-requests";
export function useEvaluation(key: string, active: boolean) {
    const [initial] = useState(() => {
        try {
            return { intent: loadEval(localStorage), invalid: false };
        }
        catch {
            return { intent: null, invalid: true };
        }
    });
    const [id, setId] = useState(() => new URL(location.href).searchParams.get("eval_id") ?? localStorage.getItem(EVAL_CURRENT) ?? ""), [view, setView] = useState<EvalData | null>(null), [report, setReport] = useState<Report | null>(null);
    const [targets, setTargets] = useState<Record<string, unknown>[]>([]), [rules, setRules] = useState<Rule[]>([]), [target, setTarget] = useState(initial.intent?.target_key ?? "target-v2"), [selected, setSelected] = useState<string[]>(initial.intent?.rules.map(r => r.name) ?? ["status_code", "exact_match"]), [repetitions, setRepetitions] = useState(initial.intent?.execution.repetitions ?? 1);
    const [pending, setPending] = useState(Boolean(initial.intent) || initial.invalid), [busy, setBusy] = useState(false), [stopping, setStopping] = useState(false), [notice, setNotice] = useState(initial.invalid ? "原评测请求记录无效" : ""), [version, setVersion] = useState(0);
    const [pair, setPair] = useState({ baseline: "", candidate: "" }), [gate, setGate] = useState<Record<string, unknown> | null>(null), [comparing, setComparing] = useState(false);
    const [catalogueNotice, setCatalogueNotice] = useState("");
    const epoch = useRef(0), compareEpoch = useRef(0), mounted = useRef(false), flight = useRef(false), stopFlight = useRef(false), compareFlight = useRef<number | null>(null), reader = useRef<AbortController | null>(null), compareReader = useRef<AbortController | null>(null), keyRef = useRef(key);
    keyRef.current = key;
    function invalidate() { epoch.current++; reader.current?.abort(); }
    function refresh() { invalidate(); setVersion(v => v + 1); }
    function select(value: string) { invalidate(); setId(value); setView(null); setReport(null); setNotice(""); localStorage.setItem(EVAL_CURRENT, value); const url = new URL(location.href); url.searchParams.set("eval_id", value); url.searchParams.set("view", "eval"); history.replaceState(null, "", url); setVersion(v => v + 1); }
    useEffect(() => { mounted.current = true; return () => { mounted.current = false; invalidate(); compareEpoch.current++; compareReader.current?.abort(); }; }, []);
    useEffect(() => { const change = () => select(new URL(location.href).searchParams.get("eval_id") ?? ""); window.addEventListener("popstate", change); return () => window.removeEventListener("popstate", change); }, []);
    useEffect(() => { compareEpoch.current++; compareReader.current?.abort(); setGate(null); }, [key, active]);
    useEffect(() => {
        if (!active)
            return;
        const controller = new AbortController();
        let live = true;
        void (async () => {
            try {
                const [a, b] = await Promise.all([request("/v1/eval-targets", key, undefined, controller.signal), request("/v1/eval-rules", key, undefined, controller.signal)]);
                if (!live)
                    return;
                if (!Array.isArray(a) || !Array.isArray(b))
                    throw new Error("目录无效");
                setTargets(a.map(object));
                setRules(b.map(r => object(r) as unknown as Rule));
                setCatalogueNotice("");
            }
            catch (error) {
                if (live)
                    setCatalogueNotice(reason(error, "目录读取未完成"));
            }
        })();
        return () => { live = false; controller.abort(); };
    }, [key, active]);
    useEffect(() => {
        invalidate();
        setView(null);
        setReport(null);
        if (!active || !id)
            return;
        setNotice("");
        const token = epoch.current, controller = new AbortController();
        reader.current = controller;
        const current = () => mounted.current && token === epoch.current && !controller.signal.aborted;
        void (async () => {
            try {
                while (current()) {
                    try {
                        const value = parseEval(await request("/v1/evals/" + encodeURIComponent(id), key, undefined, controller.signal));
                        if (!current())
                            return;
                        if (value.id !== id)
                            throw new Error("评测身份不一致");
                        const next = parseReport(await request(`/v1/evals/${encodeURIComponent(id)}/report?revision=${value.revision}`, key, undefined, controller.signal));
                        if (!current())
                            return;
                        if (next.eval_id !== id || next.revision !== value.revision)
                            throw new Error("报告身份不一致");
                        setView(value);
                        setReport(next);
                        if (!["queued", "running"].includes(value.status))
                            return;
                    }
                    catch (error) {
                        if (!(error instanceof RequestError && error.code === "snapshot_changed"))
                            throw error;
                    }
                    await new Promise<void>(resolve => { const finish = () => { clearTimeout(timer); controller.signal.removeEventListener("abort", finish); resolve(); }; const timer = setTimeout(finish, 150); controller.signal.addEventListener("abort", finish, { once: true }); });
                }
            }
            catch (error) {
                if (current())
                    setNotice(reason(error, "评测读取未完成"));
            }
        })();
        return () => controller.abort();
    }, [id, key, active, version]);
    async function start(dataset: Dataset | null) {
        if (flight.current)
            return;
        flight.current = true;
        setBusy(true);
        setNotice("");
        const token = epoch.current, capturedKey = key;
        try {
            const saved = loadEval(localStorage);
            if (!saved && !dataset)
                throw new Error("请先选择修订");
            const body: EvalBody = saved ?? { request_key: crypto.randomUUID(), dataset: { name: dataset!.name, version: dataset!.version, fingerprint: dataset!.fingerprint }, target_key: target, rules: selected.map(name => {
                    const value = rules.find(r => r.name === name);
                    if (!value)
                        throw new Error("规则目录未就绪");
                    return value;
                }), execution: { repetitions, concurrency: 1, item_timeout_seconds: 12, scorer_timeout_seconds: 1, batch_timeout_seconds: 600 } };
            const value = await evalOrFind(localStorage, body, key);
            if (mounted.current && token === epoch.current && keyRef.current === capturedKey)
                select(value.id);
        }
        catch (error) {
            if (mounted.current && keyRef.current === capturedKey)
                setNotice(localStorage.getItem(EVAL_INTENT) ? "评测创建结果未确认" : reason(error, error instanceof Error ? error.message : "评测未创建"));
        }
        finally {
            flight.current = false;
            if (mounted.current) {
                setBusy(false);
                setPending(localStorage.getItem(EVAL_INTENT) !== null);
            }
        }
    }
    async function stop() {
        if (!id || stopFlight.current)
            return;
        stopFlight.current = true;
        setStopping(true);
        const token = epoch.current, currentId = id;
        try {
            const value = parseEval(await request(`/v1/evals/${encodeURIComponent(currentId)}/cancel`, key, {}));
            if (value.id !== currentId)
                throw new Error("评测身份不一致");
            if (mounted.current && token === epoch.current) {
                setView(value);
                refresh();
            }
        }
        catch (error) {
            if (mounted.current && token === epoch.current)
                setNotice(reason(error, "停止结果未确认"));
        }
        finally {
            stopFlight.current = false;
            if (mounted.current)
                setStopping(false);
        }
    }
    function changePair(field: "baseline" | "candidate", value: string) { compareEpoch.current++; compareReader.current?.abort(); setGate(null); setPair(old => ({ ...old, [field]: value })); setComparing(false); }
    async function compare() {
        const token = compareEpoch.current;
        if (compareFlight.current === token)
            return;
        compareFlight.current = token;
        compareReader.current?.abort();
        const controller = new AbortController();
        compareReader.current = controller;
        setComparing(true);
        setGate(null);
        setNotice("");
        const current = () => mounted.current && token === compareEpoch.current && !controller.signal.aborted;
        try {
            const [a, b] = await Promise.all([request(`/v1/evals/${encodeURIComponent(pair.baseline)}/report`, key, undefined, controller.signal), request(`/v1/evals/${encodeURIComponent(pair.candidate)}/report`, key, undefined, controller.signal)]);
            if (!current())
                return;
            const before = parseReport(a), after = parseReport(b);
            if (before.eval_id !== pair.baseline || after.eval_id !== pair.candidate)
                throw new Error("报告身份不一致");
            const query = new URLSearchParams({ baseline: pair.baseline, candidate: pair.candidate, baseline_revision: String(before.revision), candidate_revision: String(after.revision), baseline_hash: before.report_hash, candidate_hash: after.report_hash });
            const value = object(await request("/v1/evals/compare?" + query, key, undefined, controller.signal));
            if (!current())
                return;
            if (!["pass", "fail", "invalid"].includes(String(value.status)))
                throw new Error("比较响应无效");
            if (value.status !== "invalid" && (object(value.baseline).eval_id !== pair.baseline || object(value.candidate).eval_id !== pair.candidate))
                throw new Error("比较身份不一致");
            setGate(value);
        }
        catch (error) {
            if (current()) {
                setGate({ status: "invalid", reasons: [{ code: "read_failed" }] });
                setNotice(reason(error, "比较读取未完成"));
            }
        }
        finally {
            if (compareFlight.current === token)
                compareFlight.current = null;
            if (current())
                setComparing(false);
        }
    }
    return { id, view, report, targets, rules, target, setTarget, selected, setSelected, repetitions, setRepetitions, pending, busy, stopping, notice: notice || catalogueNotice, select, refresh, start, stop, pair, gate, comparing, changePair, compare };
}
export type EvaluationState = ReturnType<typeof useEvaluation>;
