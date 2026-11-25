import { useEffect, useRef, useState } from "react";
import { request, RequestError } from "../api";
import type { SessionState } from "./use-session";
import { emptySidebar, parseSidebar, type SidebarLayout } from "../lib/session-sidebar";

export function useSessionSidebar(chat: SessionState, apiKey: string) {
  const scope = JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.ready]);
  const current = useRef(scope); current.current = scope;
  const [loaded, setLoaded] = useState<{ scope: string; layout: SidebarLayout } | null>(null);
  const [message, setMessage] = useState<{ scope: string; text: string } | null>(null);
  const [pending, setPending] = useState<string | null>(null), [version, setVersion] = useState(0);
  const flight = useRef(false), mounted = useRef(false), epoch = useRef(0);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    const changed = (event: Event) => { const detail = (event as CustomEvent<{ principal: string; workspace: string }>).detail; if (detail?.principal === chat.actor?.principal_id && detail.workspace === chat.workspace) setVersion(value => value + 1); };
    window.addEventListener("krim:sidebar-changed", changed); return () => window.removeEventListener("krim:sidebar-changed", changed);
  }, [chat.actor?.principal_id, chat.workspace]);
  const path = `/v2/workspaces/${encodeURIComponent(chat.workspace)}/session-sidebar`;
  useEffect(() => {
    if (!chat.ready || !chat.workspace) return;
    const controller = new AbortController(), captured = scope, generation = ++epoch.current;
    setLoaded(previous => previous?.scope === captured ? previous : null); setPending(null); flight.current = false;
    void request(path, apiKey, undefined, controller.signal).then(raw => {
      if (!mounted.current || controller.signal.aborted || current.current !== captured || epoch.current !== generation) return;
      const layout = parseSidebar(raw); setLoaded({ scope: captured, layout }); setMessage(null);
    }).catch(() => { if (mounted.current && !controller.signal.aborted && current.current === captured && epoch.current === generation) setMessage({ scope: captured, text: "无法读取会话排序，请重新读取" }); });
    return () => controller.abort();
  }, [scope, path, apiKey, version, chat.ready, chat.workspace]);
  const value = loaded?.scope === scope ? loaded.layout : emptySidebar();
  // 服务端在 GET 时按真实绑定补齐组（项目）与会话；缺项只等下一次重读，不再本地拼组。
  const layout = value;
  async function save(next: SidebarLayout) {
    if (!chat.ready || !chat.canWrite || loaded?.scope !== scope || flight.current || pending === scope) return false;
    const captured = scope, generation = epoch.current, prior = loaded.layout, body = { expected_revision: prior.revision, groups: next.groups, sessions: next.sessions };
    const valid = () => mounted.current && current.current === captured && epoch.current === generation;
    flight.current = true; setPending(captured); setMessage(null);
    try {
      const result = parseSidebar(await request(path, apiKey, body, undefined, "PUT"));
      if (!valid()) return false;
      setLoaded({ scope: captured, layout: result }); setPending(null); return true;
    } catch (error) {
      if (!valid()) return false;
      // Never replay an uncertain mutation. Read back the authoritative snapshot.
      try {
        const result = parseSidebar(await request(path, apiKey));
        if (!valid()) return false;
        setLoaded({ scope: captured, layout: result }); setPending(null);
        if (JSON.stringify(result.groups) === JSON.stringify(next.groups) && JSON.stringify(result.sessions) === JSON.stringify(next.sessions)) return true;
        setMessage({ scope: captured, text: error instanceof RequestError && error.status === 409 ? "排序已在其他页面更新，已重新读取" : "本次排序未保存，请重新操作" });
      } catch { if (valid()) setMessage({ scope: captured, text: "排序结果尚未确认，请重新读取后再操作" }); }
      return false;
    } finally { if (valid()) flight.current = false; }
  }
  return { layout, save, ready: loaded?.scope === scope, saving: pending === scope, message: message?.scope === scope ? message.text : "", reload: () => setVersion(value => value + 1) };
}
