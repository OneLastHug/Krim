import { RefreshCw, Square } from "lucide-react";
import { Button } from "./ui/button";
import { useSession } from "../hooks/use-session";
import { SessionList } from "./SessionList";
import { ChatHistory } from "./ChatHistory";
import { ChatComposer } from "./ChatComposer";
import { chatCopy as c, turnLabels } from "../lib/copy";

export function ChatWorkbench({ apiKey, active, fence, onLegacy, httpEnabled, onConnect }: { apiKey: string; active: boolean; fence: { current: number }; onLegacy: (value: boolean) => void; httpEnabled: boolean; onConnect: () => void }) {
  const chat = useSession(apiKey, active, fence, onLegacy);
  return <section hidden={!active} className="chat-workbench" aria-label="对话工作台">
    <SessionList chat={chat} httpEnabled={httpEnabled} />
    <section className="chat-main" aria-labelledby="chat-heading">
      <header className="chat-heading"><div><h2 id="chat-heading" data-ui-data>{chat.session?.title ?? c.tab}</h2><div className="chat-current-meta"><span role="status" id="chat-status">{chat.session?.status === "closed" ? "已关闭" : chat.session?.dispatch_paused ? "已暂停" : chat.currentTurn ? turnLabels[chat.currentTurn.status] : chat.reading ? "读取中" : ""}</span>{chat.id && <details><summary data-ui-data className="mono">{chat.id.slice(0, 8)}</summary><code data-ui-data>{chat.id}</code></details>}</div></div>
        <div className="chat-current-actions"><Button variant="ghost" aria-label="刷新对话" onClick={chat.refresh}><RefreshCw aria-hidden="true" /></Button>{chat.currentTurn && ["queued", "running"].includes(chat.currentTurn.status) && <Button id="chat-stop" variant="outline" disabled={!chat.canWrite || chat.controlling} onClick={() => void chat.control(chat.currentTurn, "cancel")}><Square aria-hidden="true" />停止</Button>}{chat.session?.dispatch_paused && <Button variant="outline" disabled={!chat.canWrite || chat.controlling} onClick={() => void chat.control(null, "resume")}>{c.resume}</Button>}{chat.session?.status === "open" && <Button variant="ghost" disabled={!chat.canWrite || chat.controlling} onClick={() => void chat.control(null, "close")}>{c.close}</Button>}</div>
      </header>
      {chat.notice && <div className="chat-notice"><p role="status">{chat.notice}</p>{chat.notice.includes("连接") && <Button variant="outline" onClick={onConnect}>连接</Button>}</div>}
      {chat.pending.length > 0 && <div className="chat-pending" role="status"><strong>{chat.pending.every(chat.posting) ? "提交中" : c.unknown}</strong>{chat.pending.map(intent => <div key={intent.id}><span data-ui-data>{intent.kind === "session" ? intent.body.title : (intent.session_id ?? "").slice(0, 8)}</span><Button variant="outline" onClick={() => void chat.recover(intent)}>{c.find}</Button></div>)}</div>}
      {chat.pendingControls.map(value => <div className="chat-pending" key={String(value.body.request_key)}><span>操作结果未确认</span><Button variant="outline" onClick={() => void chat.recoverControl(value)}>{value.rejected ? "重试原操作" : "查找操作结果"}</Button></div>)}
      <ChatHistory chat={chat} />
      <ChatComposer chat={chat} />
    </section>
  </section>;
}
