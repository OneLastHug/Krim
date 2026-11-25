import { useRef, useState } from "react";
import { Check, ChevronDown, Search, Settings2 } from "lucide-react";
import { PopoverMenu } from "../src/components/popover-menu";
import { useGatewayPreview, type GatewayModelLevel } from "./gateway-state";
import "./gateway-controls.css";

function levelDetail(level: GatewayModelLevel): string {
  const parts = [level.effort ? `effort ${level.effort}` : "effort 省略"];
  if (level.thinking !== "omit") parts.push(level.thinking === "enabled" ? "thinking on" : "thinking off");
  return parts.join(" · ");
}

export function GatewayControls({ sessionId }: { sessionId: string }) {
  const gateway = useGatewayPreview();
  const selection = gateway.forSession(sessionId);
  const selected = gateway.models.find(item => item.id === selection.modelId);
  const selectedLevel = selected?.levels.find(item => item.id === selection.levelId);
  const [open, setOpen] = useState<"model" | "extra" | null>(null);
  const [query, setQuery] = useState("");
  const modelRef = useRef<HTMLButtonElement>(null), extraRef = useRef<HTMLButtonElement>(null);
  const keyword = query.trim().toLowerCase();
  const groups = gateway.gateways
    .map(connection => ({
      connection,
      models: gateway.models.filter(model => model.gatewayId === connection.id && (!keyword
        || model.displayName.toLowerCase().includes(keyword)
        || model.modelId.toLowerCase().includes(keyword)
        || connection.name.toLowerCase().includes(keyword))),
    }))
    .filter(group => group.models.length > 0);
  const firstMatch = groups[0]?.models[0];
  function toggleModel() {
    if (open === "model") { setOpen(null); return; }
    setQuery("");
    setOpen("model");
  }
  function pickModel(id: string) { gateway.selectModel(id, sessionId); setOpen(null); }
  function pickLevel(id: string | null) { gateway.selectLevel(id, sessionId); setOpen(null); }
  function manage() { setOpen(null); window.dispatchEvent(new Event("krim:gateway-settings")); }
  return <>
    <button type="button" ref={modelRef} className="control-chip is-strong" aria-label="选择模型" aria-haspopup="menu" aria-expanded={open === "model"} onClick={toggleModel}><span className="control-model-name" title={selected?.modelId}>{selected?.displayName ?? "选择模型"}</span><ChevronDown aria-hidden="true"/></button>
    <button type="button" ref={extraRef} className="control-chip" aria-label="选择推理强度" aria-haspopup="menu" aria-expanded={open === "extra"} title={selectedLevel ? `推理强度：${selectedLevel.label}` : "使用模型默认推理配置"} onClick={() => setOpen(open === "extra" ? null : "extra")}>Extra<ChevronDown aria-hidden="true"/></button>
    <PopoverMenu open={open === "model"} anchor={modelRef.current} placement="above" onClose={() => setOpen(null)} label="选择模型" className="gateway-model-menu">
      <div className="gateway-menu-search">
        <Search aria-hidden="true"/>
        <input value={query} placeholder="搜索模型或网关" aria-label="搜索模型" autoComplete="off" spellCheck={false} autoFocus
          onChange={event => setQuery(event.target.value)}
          onKeyDown={event => { if (event.key === "Enter" && firstMatch) pickModel(firstMatch.id); }}/>
      </div>
      <div className="gateway-menu-body">
        {groups.map(({ connection, models }) => <div className="gateway-menu-group" key={connection.id}>
          <p className="gateway-menu-heading">{connection.name}</p>
          {models.map(model => {
            const picked = selected?.id === model.id;
            return <button type="button" key={model.id} className={`menu-item gateway-choice${picked ? " is-selected" : ""}`} role="menuitemradio" aria-checked={picked} title={model.modelId} onClick={() => pickModel(model.id)}>
              <span className="gateway-check">{picked && <Check aria-hidden="true"/>}</span>
              <span className="gateway-choice-body"><strong>{model.displayName}</strong><small>{model.modelId}</small></span>
            </button>;
          })}
        </div>)}
        {groups.length === 0 && <p className="gateway-menu-hint">{gateway.models.length ? "没有匹配的模型" : "尚未添加模型"}</p>}
      </div>
      <hr className="menu-separator"/>
      <button type="button" className="menu-item" role="menuitem" onClick={manage}><Settings2 aria-hidden="true"/>管理接入与模型</button>
    </PopoverMenu>
    <PopoverMenu open={open === "extra"} anchor={extraRef.current} placement="above" onClose={() => setOpen(null)} label="选择推理强度" className="gateway-reasoning-menu">
      <p className="gateway-menu-heading">{selected ? `推理强度 · ${selected.displayName}` : "推理强度"}</p>
      <button type="button" className="menu-item gateway-choice" role="menuitemradio" aria-checked={!selection.levelId} onClick={() => pickLevel(null)}>
        <span className="gateway-check">{!selection.levelId && <Check aria-hidden="true"/>}</span>
        <span className="gateway-choice-body"><strong>模型默认</strong><small>不发送额外推理参数</small></span>
      </button>
      {selected?.levels.map(level => {
        const picked = selection.levelId === level.id;
        return <button type="button" key={level.id} className={`menu-item gateway-choice${picked ? " is-selected" : ""}`} role="menuitemradio" aria-checked={picked} onClick={() => pickLevel(level.id)}>
          <span className="gateway-check">{picked && <Check aria-hidden="true"/>}</span>
          <span className="gateway-choice-body"><strong>{level.label}</strong><small>{levelDetail(level)}</small></span>
        </button>;
      })}
      {selected && selected.levels.length === 0 && <p className="gateway-menu-hint">此模型未配置推理档位</p>}
      <p className="gateway-menu-hint">仅影响当前会话的后续消息。</p>
      <hr className="menu-separator"/>
      <button type="button" className="menu-item" role="menuitem" onClick={manage}><Settings2 aria-hidden="true"/>配置推理档位</button>
    </PopoverMenu>
  </>;
}
