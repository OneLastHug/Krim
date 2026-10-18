import { ui } from "./lib/copy";
import { useEffect, useState } from "react";
import { object, request } from "./api";
import { RunComposer } from "./components/RunComposer";
import { RunAccess } from "./components/RunAccess";
import { RunWorkspace } from "./components/RunWorkspace";
import { useRun } from "./hooks/use-run";
import { EvaluationWorkbench } from "./components/EvaluationWorkbench";
import { Tabs, TabsList, TabsTrigger } from "./components/ui/tabs";
export function App() {
    const [key, setKey] = useState(""), [keyOpen, setKeyOpen] = useState(false);
    const run = useRun(key);
    const [httpEnabled, setHttpEnabled] = useState<boolean | null>(null);
    useEffect(() => {
        let current = true;
        const controller = new AbortController();
        setHttpEnabled(null);
        void request("/healthz", key, undefined, controller.signal).then(raw => {
            if (current) {
                const value = object(raw).http_enabled;
                setHttpEnabled(typeof value === "boolean" ? value : null);
            }
        }).catch(() => { if (current) setHttpEnabled(null); });
        return () => { current = false; controller.abort(); };
    }, [key]);
    const [mode, setMode] = useState(() => new URL(location.href).searchParams.get("view") === "eval" ? "eval" : "run"), [sourceRun, setSourceRun] = useState("");
    function choose(value: string) {
        setMode(value);
        const url = new URL(location.href);
        if (value === "eval")
            url.searchParams.set("view", "eval");
        else
            url.searchParams.delete("view");
        history.replaceState(null, "", url);
    }
    return <main className="app-shell"><header className="app-header"><div className="brand"><h1>{ui.brand}</h1><span>{httpEnabled === false ? ui.offline : ui.agent}</span></div><RunAccess apiKey={key} onKey={value => { run.invalidate(); setKey(value); run.refresh(); }} onOpen={id => { run.select(id); choose("run"); }} keyOpen={keyOpen} setKeyOpen={setKeyOpen}/></header><Tabs className="app-nav" value={mode} onValueChange={value => choose(String(value))}><TabsList variant="line" aria-label="工作台"><TabsTrigger value="run">任务</TabsTrigger><TabsTrigger value="eval">评测</TabsTrigger></TabsList></Tabs><section hidden={mode !== "run"}><div className="workbench"><RunComposer run={run} onConnect={() => setKeyOpen(true)}/><RunWorkspace key={run.id} run={run} onCandidate={id => { setSourceRun(id); choose("eval"); }}/></div></section><EvaluationWorkbench apiKey={key} active={mode === "eval"} sourceRun={sourceRun} onRun={id => { run.select(id); choose("run"); }}/></main>;
}
