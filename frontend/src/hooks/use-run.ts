import { useEffect, useRef, useState } from "react";
import { object, parseRun, request, isUnadmittedCreate, type RunData } from "../api";
import { createOrFind, CURRENT, initialIntent, INTENT, replyApproval } from "../lib/run-requests";
import { codeOf, reason, ui } from "../lib/copy";
export interface Notice {
    text: string;
    code: string | null;
    area: "composer" | "run" | "approval";
}
const terminal = (status: string) => !["queued", "running"].includes(status);
function pause(signal: AbortSignal) { return new Promise<void>(resolve => { const finish = () => { clearTimeout(timer); signal.removeEventListener("abort", finish); resolve(); }; const timer = setTimeout(finish, 100); signal.addEventListener("abort", finish, { once: true }); }); }
export function useRun(key: string) {
    const [initial] = useState(initialIntent);
    const [draft, setDraft] = useState(initial.intent?.text ?? "");
    const [pending, setPending] = useState(Boolean(initial.intent) || initial.invalid);
    const [id, setId] = useState(() => location.hash.slice(1) || localStorage.getItem(CURRENT) || "");
    const [view, setView] = useState<RunData | null>(null), [facts, setFacts] = useState<Record<string, unknown> | null>(null), [trace, setTrace] = useState<Record<string, unknown> | null>(null);
    const [events, setEvents] = useState<Record<string, unknown>[]>([]), [approvals, setApprovals] = useState<Record<string, unknown>[]>([]);
    const [reading, setReading] = useState(Boolean(id)), [version, setVersion] = useState(0);
    const [notice, setNotice] = useState<Notice | null>(initial.invalid ? { text: ui.invalidRecord, area: "composer", code: null } : null);
    const [submitting, setSubmitting] = useState(false), [stopping, setStopping] = useState(false), [replying, setReplying] = useState(false);
    const mounted = useRef(false), epoch = useRef(0), reader = useRef<AbortController | null>(null);
    const createFlight = useRef(false), stopFlight = useRef(false), replyFlight = useRef(false);
    function invalidate() { epoch.current++; reader.current?.abort(); }
    function select(next: string) { invalidate(); setId(next); setView(null); setFacts(null); setTrace(null); setEvents([]); setApprovals([]); setReading(true); setNotice(null); location.hash = next; localStorage.setItem(CURRENT, next); setVersion(v => v + 1); }
    function refresh() { invalidate(); setNotice(value => value?.area === "run" ? null : value); setVersion(v => v + 1); }
    useEffect(() => { mounted.current = true; return () => { mounted.current = false; invalidate(); }; }, []);
    useEffect(() => { const onHash = () => { const next = location.hash.slice(1); if (next !== id) {
        invalidate();
        setId(next);
        setVersion(v => v + 1);
    } }; window.addEventListener("hashchange", onHash); return () => window.removeEventListener("hashchange", onHash); }, [id]);
    useEffect(() => {
        const token = ++epoch.current, controller = new AbortController();
        reader.current = controller;
        const current = () => mounted.current && token === epoch.current && !controller.signal.aborted;
        setView(null);
        setFacts(null);
        setTrace(null);
        setEvents([]);
        setApprovals([]);
        setReading(Boolean(id));
        if (!id)
            return () => controller.abort();
        async function watch() {
            let cursor = 0;
            const timeline: Record<string, unknown>[] = [];
            try {
                while (current()) {
                    const run = parseRun(await request("/v1/runs/" + encodeURIComponent(id), key, undefined, controller.signal));
                    if (!current())
                        return;
                    if (run.id !== id)
                        throw new Error("identity");
                    let records;
                    do {
                        records = await request(`/v1/runs/${encodeURIComponent(id)}/events?from_seq=${cursor}`, key, undefined, controller.signal);
                        if (!current())
                            return;
                        if (!Array.isArray(records))
                            throw new Error("events");
                        for (const raw of records) {
                            const event = object(raw);
                            if (event.run_id !== id || event.seq !== cursor)
                                throw new Error("event_identity");
                            timeline.push(event);
                            cursor++;
                        }
                    } while (records.length === 64 && current());
                    const raw = await request(`/v1/runs/${encodeURIComponent(id)}/approvals`, key, undefined, controller.signal);
                    if (!current())
                        return;
                    if (!Array.isArray(raw))
                        throw new Error("approvals");
                    const requests = raw.map(object);
                    if (requests.some(r => r.run_id !== id))
                        throw new Error("approval_identity");
                    setView(run);
                    setEvents([...timeline]);
                    setApprovals(requests);
                    setReading(false);
                    if (terminal(run.status)) {
                        const [a, b] = await Promise.all([request(`/v1/runs/${encodeURIComponent(id)}/result`, key, undefined, controller.signal), request(`/v1/runs/${encodeURIComponent(id)}/trace`, key, undefined, controller.signal)]);
                        if (!current())
                            return;
                        const result = object(a), projected = object(b);
                        if (parseRun(result.run).id !== id || projected.run_id !== id)
                            throw new Error("result_identity");
                        setFacts(result);
                        setTrace(projected);
                        return;
                    }
                    await pause(controller.signal);
                }
            }
            catch (error) {
                if (current())
                    setNotice({ area: "run", text: reason(error, ui.readFailed), code: codeOf(error) });
            }
            finally {
                if (current())
                    setReading(false);
            }
        }
        void watch();
        return () => controller.abort();
    }, [id, key, version]);
    async function submit() {
        if (createFlight.current)
            return;
        createFlight.current = true;
        setPending(true);
        setSubmitting(true);
        setNotice(null);
        const token = epoch.current, current = () => mounted.current && token === epoch.current;
        try {
            const run = await createOrFind(localStorage, draft, key, current);
            if (current()) {
                setPending(false);
                select(run.id);
            }
        }
        catch (error) {
            if (current()) {
                const unknown = localStorage.getItem(INTENT) !== null;
                setPending(unknown);
                setNotice({ area: "composer", text: !unknown && isUnadmittedCreate(error) ? reason(error) : ui.unknownCreate, code: codeOf(error) });
            }
        }
        finally {
            createFlight.current = false;
            if (mounted.current) {
                const stored = initialIntent();
                setPending(Boolean(stored.intent) || stored.invalid);
                if (stored.intent) setDraft(stored.intent.text);
                setSubmitting(false);
            }
        }
    }
    async function stop() {
        if (stopFlight.current || !id)
            return;
        stopFlight.current = true;
        setStopping(true);
        setNotice(null);
        const token = epoch.current, target = id;
        try {
            const run = parseRun(await request(`/v1/runs/${encodeURIComponent(target)}/cancel`, key, {}));
            if (run.id !== target)
                throw new Error("identity");
            if (mounted.current && token === epoch.current) {
                setView(run);
                refresh();
            }
        }
        catch (error) {
            if (mounted.current && token === epoch.current)
                setNotice({ area: "run", text: ui.unknownStop, code: codeOf(error) });
        }
        finally {
            stopFlight.current = false;
            if (mounted.current)
                setStopping(false);
        }
    }
    async function reply(approval: Record<string, unknown>, allow: boolean) {
        if (replyFlight.current || !id)
            return;
        replyFlight.current = true;
        setReplying(true);
        setNotice(null);
        const token = epoch.current, target = id;
        try {
            await replyApproval(localStorage, target, approval, allow, key);
            if (mounted.current && token === epoch.current)
                refresh();
        }
        catch (error) {
            if (mounted.current && token === epoch.current)
                setNotice({ area: "approval", text: reason(error, ui.unknownApproval), code: codeOf(error) });
        }
        finally {
            replyFlight.current = false;
            if (mounted.current)
                setReplying(false);
        }
    }
    return { draft, setDraft, pending, id, view: view?.id === id ? view : null, facts, trace, events, approvals, reading, notice, submitting, stopping, replying, select, refresh, invalidate, submit, stop, reply };
}
export type RunState = ReturnType<typeof useRun>;
