import { ui } from "../lib/copy";
import { useEffect, useRef, useState } from "react";
import { RequestError, request } from "../api";
import { parseActor } from "../lib/session-contracts";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";

export function RunAccess({ apiKey, onKey, onOpen, keyOpen, setKeyOpen, taskOpen, setTaskOpen, isLegacy = true }: {
  apiKey: string;
  onKey: (key: string) => void;
  onOpen: (id: string) => void;
  keyOpen: boolean;
  setKeyOpen: (open: boolean) => void;
  taskOpen: boolean;
  setTaskOpen: (open: boolean) => void;
  isLegacy?: boolean;
}) {
  const [id, setId] = useState(""), [draft, setDraft] = useState("");
  const [checking, setChecking] = useState(false), [error, setError] = useState("");
  const reader = useRef<AbortController | null>(null);
  const epoch = useRef(0), open = useRef(keyOpen), sourceKey = useRef(apiKey);
  open.current = keyOpen; sourceKey.current = apiKey;
  useEffect(() => {
    epoch.current++; reader.current?.abort(); setDraft(""); setError(""); setChecking(false);
    return () => { epoch.current++; reader.current?.abort(); };
  }, [keyOpen, apiKey]);
  function closeKey(next: boolean) {
    if (!next) { epoch.current++; reader.current?.abort(); setDraft(""); setError(""); setChecking(false); }
    setKeyOpen(next);
  }
  async function login() {
    const value = draft.trim(); if (!value || checking) return;
    const controller = new AbortController(); reader.current?.abort(); reader.current = controller;
    const stamp = ++epoch.current, previous = apiKey;
    const owns = () => stamp === epoch.current && open.current && !controller.signal.aborted && sourceKey.current === previous;
    setChecking(true); setError("");
    try {
      parseActor(await request("/v2/me", value, undefined, controller.signal));
      if (!owns()) return;
      setDraft(""); onKey(value); closeKey(false);
    } catch (cause) {
      if (!owns()) return;
      setError(cause instanceof RequestError && cause.status === 401
        ? "密钥无效或账号已停用，请检查后重试。"
        : "无法连接服务，请检查连接后重试。");
    } finally { if (owns()) setChecking(false); }
  }
  return <nav aria-label={ui.taskActions} className="access-actions">
    {isLegacy && <Dialog open={taskOpen} onOpenChange={setTaskOpen}>
      <DialogContent aria-describedby={undefined}><DialogHeader><DialogTitle>{ui.openTask}</DialogTitle></DialogHeader>
        <form onSubmit={event => { event.preventDefault(); if (id.trim()) { onOpen(id.trim()); setTaskOpen(false); } }}>
          <Label htmlFor="history-id">{ui.taskID}</Label><Input id="history-id" value={id} onChange={event => setId(event.target.value)} placeholder={ui.pasteID} required autoComplete="off"/>
          <div className="dialog-actions"><Button type="submit">{ui.open}</Button></div>
        </form>
      </DialogContent>
    </Dialog>}
    <Dialog open={keyOpen} onOpenChange={closeKey}>
      <DialogContent aria-describedby="access-key-description"><DialogHeader><DialogTitle>{apiKey ? "切换账号" : "登录 Krim"}</DialogTitle></DialogHeader>
        <p id="access-key-description" className="access-key-description">使用管理员为你签发的访问密钥登录。</p>
        <form onSubmit={event => { event.preventDefault(); void login(); }}>
          <Label htmlFor="api-key">访问密钥</Label><Input id="api-key" type="password" value={draft} onChange={event => setDraft(event.target.value)} autoComplete="off" maxLength={256} required disabled={checking}/>
          {error && <p className="access-key-feedback" role="alert">{error}</p>}
          <div className="dialog-actions"><Button variant="outline" type="button" onClick={() => closeKey(false)}>取消</Button><Button id="connect-save" type="submit" disabled={checking || !draft.trim()}>{checking ? "正在验证…" : apiKey ? "切换账号" : "登录"}</Button></div>
        </form>
      </DialogContent>
    </Dialog>
  </nav>;
}
