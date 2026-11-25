/* 正式聊天底栏的模型与推理档位控制：样式与交互沿用 gateway-preview/GatewayControls 样板，
   数据来自 useModelGateways；两个 control-chip 的位置、样式与图标保持原样。 */
import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown, Search, Settings2 } from "lucide-react";
import { PopoverMenu } from "./popover-menu";
import { EMPTY_SELECTION, gatewayError, parseSessionModelSelection, modelKey, toWireSelection, type GatewaySelection, type SessionModelSelection } from "../lib/model-gateways";
import { useModelGateways } from "../hooks/use-model-gateways";
import { request } from "../api";
import "./model-gateway-controls.css";

export type SelectionSetter = (selection: GatewaySelection | null) => void;
type Info = "model" | "extra" | null;

function levelDetail(level: { effort: string; thinking: string }): string {
  const parts = [level.effort ? `effort ${level.effort}` : "effort 省略"];
  if (level.thinking !== "omit") parts.push(level.thinking === "enabled" ? "thinking on" : "thinking off");
  return parts.join(" · ");
}

/** 会话模型选择读取：GET 端点返回冻结配置与当前 revision。 */
export async function fetchSessionModelSelection(path: string, key: string): Promise<SessionModelSelection> {
  return parseSessionModelSelection(await request(path, key));
}

function selectionFromRemote(value: SessionModelSelection | undefined): GatewaySelection {
  const wire = value?.model_selection;
  if (!wire) return EMPTY_SELECTION;
  // 服务端冻结的模型可能已被移除：仍显示 wire ID，不静默改回默认。
  return { modelId: modelKey(wire.gateway_id, wire.model_id), levelId: wire.reasoning_level };
}

export function ModelGatewayControls({ apiKey, chat, sessionPath, frozen, onNewSelection, newSelection }: {
  apiKey: string;
  chat: { id: string; workspace: string; session: { revision: number; owner_id: string } | null; actor: { principal_id: string; generation: number } | null; canWrite: boolean; running: boolean; refresh: () => void };
  sessionPath: string;
  frozen: { model: string | null; effort: string | null; thinking: string | null };
  onNewSelection: SelectionSetter;
  newSelection: GatewaySelection | null;
}) {
  const gateway = useModelGateways();
  const [open, setOpen] = useState<Info>(null);
  const [query, setQuery] = useState("");
  const [notice, setNotice] = useState("");
  const [remote, setRemote] = useState<{ path: string; value: SessionModelSelection } | null>(null);
  const [busy, setBusy] = useState(false);
  const modelRef = useRef<HTMLButtonElement>(null), extraRef = useRef<HTMLButtonElement>(null);
  const live = useRef(true), epoch = useRef(0);
  const sessionPathRef = useRef(sessionPath); sessionPathRef.current = sessionPath;
  const requestScope = JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, sessionPath]);
  const requestScopeRef = useRef(requestScope); requestScopeRef.current = requestScope;

  useEffect(() => {
    live.current = true;
    return () => { live.current = false; epoch.current++; };
  }, []);

  // 既有会话：读取服务端模型选择（含冻结配置回执）；新会话不发请求。
  const readKey = chat.session ? sessionPath : "";
  useEffect(() => {
    if (!readKey) { setRemote(null); return; }
    const ticket = ++epoch.current;
    setRemote(null);
    void (async () => {
      try {
        const value = await fetchSessionModelSelection(readKey, apiKey);
        if (!live.current || ticket !== epoch.current || readKey !== sessionPathRef.current) return;
        setRemote({ path: readKey, value }); setNotice("");
      } catch (error) {
        if (!live.current || ticket !== epoch.current || readKey !== sessionPathRef.current) return;
        setRemote(null);
        setNotice(gatewayError(error));
      }
    })();
  }, [readKey, apiKey, gateway?.revision, chat.session?.revision]);

  const currentRemote = remote?.path === sessionPath ? remote.value : undefined;
  const frozenModel = currentRemote?.model ?? frozen.model ?? null;
  const models = gateway?.models ?? [];
  const connections = gateway?.gateways ?? [];
  const defaultSelection = gateway?.selection ?? EMPTY_SELECTION;
  const effective = chat.session ? selectionFromRemote(currentRemote) : newSelection ?? defaultSelection;
  const selected = models.find(item => item.id === effective.modelId) ?? null;
  const selectedLevel = selected?.levels.find(item => item.id === effective.levelId) ?? null;
  const chipModel = selected?.displayName ?? currentRemote?.model ?? frozenModel ?? "模型信息";
  const owner = !!chat.session && chat.session.owner_id === chat.actor?.principal_id;
  const disabled = !chat.canWrite || chat.running || busy || gateway?.loading === true || (!!chat.session && !owner);
  const keyword = query.trim().toLowerCase();
  const groups = connections
    .map(connection => ({
      connection,
      models: models.filter(model => model.gatewayId === connection.id && (!keyword
        || model.displayName.toLowerCase().includes(keyword)
        || model.modelId.toLowerCase().includes(keyword)
        || connection.name.toLowerCase().includes(keyword))),
    }))
    .filter(group => group.models.length > 0);
  const firstMatch = groups[0]?.models[0];
  const revision = chat.session ? Math.max(chat.session.revision, currentRemote?.revision ?? 0) : null;

  // 侧栏接入名：自定义模型 target fingerprint 与主配置接入档不匹配时，
  // App 无法从 connection profile 读出接入名；这里按会话选择广播供应商名。
  const remoteWire = currentRemote?.model_selection;
  const selectedGatewayName = selected
    ? connections.find(item => item.id === selected.gatewayId)?.name ?? selected.gatewayId
    : remoteWire
      ? connections.find(item => item.id === remoteWire.gateway_id)?.name ?? remoteWire.gateway_id
      : !chat.session && effective.modelId
        ? connections.find(item => item.id === (models.find(model => model.id === effective.modelId)?.gatewayId ?? ""))?.name ?? null
        : null;
  const labelScope = JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, selectedGatewayName]);
  useEffect(() => {
    if (!gateway || gateway.loading || !chat.actor) return;
    window.dispatchEvent(new CustomEvent("krim:session-model-label", { detail: {
      principal_id: chat.actor.principal_id, generation: chat.actor.generation,
      workspace_id: chat.workspace, session_id: chat.id, gateway_name: selectedGatewayName,
    } }));
  }, [labelScope, gateway?.loading, !!gateway, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, selectedGatewayName]);

  function toggleModel() {
    if (open === "model") { setOpen(null); return; }
    setQuery(""); setNotice("");
    setOpen("model");
  }
  function pickLevel(levelId: string | null) {
    if (!effective.modelId) return;
    void apply({ ...effective, levelId });
  }
  async function apply(selection: GatewaySelection) {
    const config = gateway;
    if (!config || disabled || busy) return;
    setOpen(null);
    if (chat.session) {
      if (revision === null) { setNotice("会话模型配置尚未读取完成，请稍后重试。"); return; }
      setBusy(true); setNotice("");
      const scopeAtStart = requestScopeRef.current;
      try {
        const wire = toWireSelection(selection, config.models);
        const saved = parseSessionModelSelection(await request(sessionPath, apiKey, {
          request_key: crypto.randomUUID(), expected_revision: revision, model_selection: wire,
        }, undefined, "PUT"));
        if (!live.current || saved.session_id !== chat.id || requestScopeRef.current !== scopeAtStart) return;
        setRemote({ path: sessionPath, value: saved });
        chat.refresh(); // revision 已推进，刷新会话防止后续 turn 用旧 revision。
      } catch (error) {
        if (!live.current || requestScopeRef.current !== scopeAtStart) return;
        setNotice(gatewayError(error));
        // 结果未确认时不自动换键重试：重新 GET 读取当前结果。
        void (async () => {
          try {
            const value = await fetchSessionModelSelection(sessionPath, apiKey);
            if (!live.current || sessionPath !== sessionPathRef.current || requestScopeRef.current !== scopeAtStart) return;
            setRemote({ path: sessionPath, value });
          } catch { /* 保留错误提示，等待下一次打开菜单重读 */ }
        })();
      } finally { if (live.current) setBusy(false); }
      return;
    }
    onNewSelection(selection);
  }
  function manage() { setOpen(null); window.dispatchEvent(new Event("krim:gateway-settings")); }
  return <>
    <button type="button" ref={modelRef} className="control-chip is-strong" aria-label="选择模型" aria-haspopup="menu" aria-expanded={open === "model"} disabled={disabled} onClick={toggleModel}><span className="control-model-name" title={chipModel}>{chipModel}</span><ChevronDown aria-hidden="true"/></button>
    <button type="button" ref={extraRef} className="control-chip" aria-label="选择推理强度" aria-haspopup="menu" aria-expanded={open === "extra"} disabled={disabled} title={selectedLevel ? `推理强度：${selectedLevel.label}` : "使用模型默认推理配置"} onClick={() => setOpen(open === "extra" ? null : "extra")}>Extra<ChevronDown aria-hidden="true"/></button>
    <PopoverMenu open={open === "model"} anchor={modelRef.current} placement="above" onClose={() => setOpen(null)} label="选择模型" className="gateway-model-menu">
      <div className="gateway-menu-search">
        <Search aria-hidden="true"/>
        <input value={query} placeholder="搜索模型或网关" aria-label="搜索模型" autoComplete="off" spellCheck={false}
          onChange={event => setQuery(event.target.value)}
          onKeyDown={event => { if (event.key === "Enter" && firstMatch) void apply({ modelId: firstMatch.id, levelId: firstMatch.levels[0]?.id ?? null }); }}/>
      </div>
      {chat.session
        ? <button type="button" className="menu-item gateway-choice" role="menuitemradio" aria-checked={!currentRemote?.model_selection} onClick={() => void apply(EMPTY_SELECTION)}>
          <span className="gateway-check">{!currentRemote?.model_selection && <Check aria-hidden="true"/>}</span>
          <span className="gateway-choice-body"><strong>主配置模型</strong><small>沿用会话目标的主配置接入，不用账号默认</small></span>
        </button>
        : <button type="button" className="menu-item gateway-choice" role="menuitemradio" aria-checked={newSelection === null} onClick={() => { setOpen(null); onNewSelection(null); }}>
          <span className="gateway-check">{newSelection === null && <Check aria-hidden="true"/>}</span>
          <span className="gateway-choice-body"><strong>使用默认模型</strong><small>沿用账号默认接入与推理档位</small></span>
        </button>}
      <div className="gateway-menu-body">
        {groups.map(({ connection, models: groupModels }) => <div className="gateway-menu-group" key={connection.id}>
          <p className="gateway-menu-heading">{connection.name}</p>
          {groupModels.map(model => {
            const picked = effective.modelId === model.id;
            return <button type="button" key={model.id} className={`menu-item gateway-choice${picked ? " is-selected" : ""}`} role="menuitemradio" aria-checked={picked} title={model.modelId} onClick={() => void apply({ modelId: model.id, levelId: model.levels[0]?.id ?? null })}>
              <span className="gateway-check">{picked && <Check aria-hidden="true"/>}</span>
              <span className="gateway-choice-body"><strong>{model.displayName}</strong><small>{model.modelId}</small></span>
            </button>;
          })}
        </div>)}
        {groups.length === 0 && <p className="gateway-menu-hint">{models.length ? "没有匹配的模型" : "尚未添加模型"}</p>}
      </div>
      <button type="button" className="menu-item" role="menuitem" onClick={manage}><Settings2 aria-hidden="true"/>管理接入与模型</button>
    </PopoverMenu>
    <PopoverMenu open={open === "extra"} anchor={extraRef.current} placement="above" onClose={() => setOpen(null)} label="选择推理强度" className="gateway-reasoning-menu">
      <p className="gateway-menu-heading">{selected ? `推理强度 · ${selected.displayName}` : "推理强度"}</p>
      {effective.modelId
        ? <button type="button" className="menu-item gateway-choice" role="menuitemradio" aria-checked={!effective.levelId} onClick={() => pickLevel(null)}>
          <span className="gateway-check">{!effective.levelId && <Check aria-hidden="true"/>}</span>
          <span className="gateway-choice-body"><strong>模型默认</strong><small>不发送额外推理参数</small></span>
        </button>
        : <button type="button" className="menu-item gateway-choice" role="menuitemradio" aria-checked={true} onClick={() => setOpen(null)}>
          <span className="gateway-check"><Check aria-hidden="true"/></span>
          <span className="gateway-choice-body"><strong>{chat.session ? "主配置模型" : "使用默认模型"}</strong><small>{chat.session ? "沿用会话目标的主配置接入，不用账号默认" : "沿用账号默认接入与推理档位"}</small></span>
        </button>}
      {selected?.levels.map(level => {
        const picked = effective.levelId === level.id;
        return <button type="button" key={level.id} className={`menu-item gateway-choice${picked ? " is-selected" : ""}`} role="menuitemradio" aria-checked={picked} onClick={() => pickLevel(level.id)}>
          <span className="gateway-check">{picked && <Check aria-hidden="true"/>}</span>
          <span className="gateway-choice-body"><strong>{level.label}</strong><small>{levelDetail(level)}</small></span>
        </button>;
      })}
      {selected && selected.levels.length === 0 && <p className="gateway-menu-hint">此模型未配置推理档位</p>}
      <hr className="menu-separator"/>
      <button type="button" className="menu-item" role="menuitem" onClick={manage}><Settings2 aria-hidden="true"/>配置推理档位</button>
    </PopoverMenu>
    {notice && <span className="gateway-control-notice" role="status">{notice}</span>}
  </>;
}
