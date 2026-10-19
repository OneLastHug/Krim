import { ui } from "./lib/copy";
import { useEffect, useRef, useState } from "react";
import { object, request } from "./api";
import { ChatWorkbench } from "./components/ChatWorkbench";
import { RunComposer } from "./components/RunComposer";
import { RunAccess } from "./components/RunAccess";
import { RunWorkspace } from "./components/RunWorkspace";
import { useRun } from "./hooks/use-run";
import { EvaluationWorkbench } from "./components/EvaluationWorkbench";
import { Tabs, TabsList, TabsTrigger } from "./components/ui/tabs";
export function App() {
    const [key, setKey] = useState(""), [keyOpen, setKeyOpen] = useState(false);
    const run = useRun(key);
    const chatFence = useRef(0), [legacy, setLegacy] = useState(true);
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
    const [mode, setMode] = useState(() => new URL(location.href).searchParams.get("view") === "eval" ? "eval" : new URL(location.href).searchParams.get("view") === "chat" ? "chat" : "run"), [sourceRun, setSourceRun] = useState("");
    function choose(value: string) {
        if (value !== mode) chatFence.current++;
        setMode(value);
        const url = new URL(location.href);
        if (value === "eval" || value === "chat")
            url.searchParams.set("view", value);
        else
            url.searchParams.delete("view");
        history.replaceState(null, "", url);
    }
    return <main className={"app-shell" + (legacy ? "" : " core-member")}><header className="app-header"><div className="brand"><h1>{ui.brand}</h1><span>{httpEnabled === false ? ui.offline : ui.agent}</span></div><RunAccess apiKey={key} onKey={value => { chatFence.current++; run.invalidate(); setKey(value); run.refresh(); }} onOpen={id => { run.select(id); choose("run"); }} keyOpen={keyOpen} setKeyOpen={setKeyOpen}/></header><Tabs className="app-nav" value={mode} onValueChange={value => choose(String(value))}><TabsList variant="line" aria-label="工作台"><TabsTrigger value="chat">对话</TabsTrigger>{legacy && <TabsTrigger value="run">任务</TabsTrigger>}{legacy && <TabsTrigger value="eval">评测</TabsTrigger>}</TabsList></Tabs><section hidden={mode !== "run"}><div className="workbench"><RunComposer run={run} onConnect={() => setKeyOpen(true)}/><RunWorkspace key={run.id} run={run} onCandidate={id => { setSourceRun(id); choose("eval"); }}/></div></section><ChatWorkbench apiKey={key} active={mode === "chat"} fence={chatFence} onLegacy={setLegacy} httpEnabled={httpEnabled === true} onConnect={() => setKeyOpen(true)}/><EvaluationWorkbench apiKey={key} active={mode === "eval"} sourceRun={sourceRun} onRun={id => { run.select(id); choose("run"); }}/></main>;
}
