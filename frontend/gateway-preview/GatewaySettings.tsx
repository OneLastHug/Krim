import { useRef, useState, type FormEvent, type ReactNode } from "react";
import { Check, ChevronDown, ChevronRight, Cpu, Eye, EyeOff, Network, Plus, Search, Server, Trash2 } from "lucide-react";
import { Button } from "../src/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogTitle } from "../src/components/ui/dialog";
import { GatewayPreviewProvider, useGatewayPreview, useOptionalGatewayPreview, type Gateway, type GatewayModel, type GatewayModelLevel, type GatewayProtocol, type GatewayThinking } from "./gateway-state";
import "./gateway-preview.css";

export interface GatewaySettingsProps { onBack?: () => void }
type Preset = { id: string; name: string; hint: string; url: string; protocol: GatewayProtocol; group: string; icon: typeof Network };
const PRESETS: Preset[] = [
  { id: "deepseek", name: "DeepSeek", hint: "api.deepseek.com", url: "https://api.deepseek.com/v1", protocol: "chat_completions", group: "API 服务", icon: Cpu },
  { id: "openai", name: "OpenAI 兼容", hint: "官方 API 或中转", url: "", protocol: "chat_completions", group: "API 服务", icon: Network },
  { id: "ollama", name: "Ollama", hint: "localhost:11434", url: "http://localhost:11434/v1", protocol: "chat_completions", group: "本地与自定义", icon: Server },
  { id: "lmstudio", name: "LM Studio", hint: "localhost:1234", url: "http://localhost:1234/v1", protocol: "chat_completions", group: "本地与自定义", icon: Server },
  { id: "custom", name: "自定义", hint: "任意兼容的 API 地址", url: "", protocol: "chat_completions", group: "本地与自定义", icon: Plus },
];
const MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;
const LEVEL_ID = /^[A-Za-z0-9._-]{1,32}$/;
const EFFORT = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/;
const PROTOCOLS = [["chat_completions", "Chat Completions"], ["responses", "Responses"]] as const;
const THINKING = [["omit", "跟随模型"], ["enabled", "开启"], ["disabled", "关闭"]] as const;

function nextId(ids: string[], prefix: string) {
  let n = 1;
  while (ids.includes(`${prefix}-${n}`)) n++;
  return `${prefix}-${n}`;
}
function hostOf(url: string) { try { return new URL(url).host; } catch { return url; } }
function ProviderIcon({ url = "" }: { url?: string }) {
  const local = /^https?:\/\/(localhost|127\.0\.0\.1|\[::1\])(?=[:/]|$)/.test(url);
  const Icon = local ? Server : /deepseek\.com/.test(url) ? Cpu : Network;
  return <Icon className="mp-provider-icon" aria-hidden="true" />;
}
function Select({ label, value, options, onChange, disabled }: { label: string; value: string; options: readonly (readonly [string, string])[]; onChange: (v: string) => void; disabled?: boolean }) {
  return <span className="mp-select"><select aria-label={label} value={value} disabled={disabled} onChange={e => onChange(e.target.value)}>{options.map(([v, text]) => <option key={v} value={v}>{text}</option>)}</select><ChevronDown aria-hidden="true" /></span>;
}
function Field({ label, id, hint, children }: { label: string; id?: string; hint?: string; children: ReactNode }) {
  return <div className="mp-field"><label htmlFor={id}>{label}</label><div className="mp-field-control">{children}{hint && <p className="mp-hint">{hint}</p>}</div></div>;
}
function Modal({ title, description, onClose, footer, children, className = "" }: { title: string; description?: string; onClose: () => void; footer?: ReactNode; children?: ReactNode; className?: string }) {
  return <Dialog open onOpenChange={open => { if (!open) onClose(); }}><DialogContent className={`mp-dialog ${className}`}>
    <header className="mp-dialog-head"><DialogTitle>{title}</DialogTitle>{description && <DialogDescription>{description}</DialogDescription>}</header>
    {children && <div className="mp-dialog-body">{children}</div>}
    {footer && <DialogFooter className="mp-dialog-foot">{footer}</DialogFooter>}
  </DialogContent></Dialog>;
}

function AddProvider({ onChoose, onClose }: { onChoose: (preset: Preset) => void; onClose: () => void }) {
  const [query, setQuery] = useState("");
  const matches = PRESETS.filter(p => `${p.name} ${p.hint}`.toLowerCase().includes(query.trim().toLowerCase()));
  return <Modal title="添加供应商" className="mp-add-dialog" onClose={onClose}>
    <label className="mp-search mp-add-search"><Search aria-hidden="true"/><input autoFocus placeholder="查找供应商…" aria-label="查找供应商" value={query} onChange={e => setQuery(e.target.value)}/></label>
    {["API 服务", "本地与自定义"].map(group => {
      const items = matches.filter(p => p.group === group);
      return items.length > 0 && <section className="mp-preset-group" key={group}><h3>{group}</h3><div className="mp-presets">{items.map(p => <button type="button" key={p.id} className={p.id === "custom" ? "mp-custom-preset" : undefined} onClick={() => onChoose(p)}><p.icon aria-hidden="true"/><span><strong>{p.name}</strong><small>{p.hint}</small></span><ChevronRight aria-hidden="true"/></button>)}</div></section>;
    })}
    {!matches.length && <p className="mp-no-results">没有匹配的供应商。可以清空搜索后选择「自定义」。</p>}
  </Modal>;
}

type ModelDraft = GatewayModel & { expanded: boolean };
type ProviderEditorProps = { initial: Gateway | null; preset?: Preset; onClose: () => void; onSaved: () => void; onRemove: () => void };
function ProviderEditor({ initial, preset, onClose, onSaved, onRemove }: ProviderEditorProps) {
  const api = useGatewayPreview();
  const [name, setName] = useState(initial?.name ?? (preset?.id === "custom" || preset?.id === "openai" ? "" : preset?.name ?? ""));
  const [url, setUrl] = useState(initial?.baseUrl ?? preset?.url ?? "");
  const [protocol, setProtocol] = useState<GatewayProtocol>(initial?.protocol ?? preset?.protocol ?? "chat_completions");
  const [key, setKey] = useState("");
  const [showKey, setShowKey] = useState(false);
  const [clearKey, setClearKey] = useState(false);
  const [models, setModels] = useState<ModelDraft[]>(() => api.models.filter(m => m.gatewayId === initial?.id).map(m => ({ ...m, levels: m.levels.map(l => ({ ...l })), expanded: false })));
  const [removedModels, setRemovedModels] = useState<string[]>([]);
  const [reasoningOpen, setReasoningOpen] = useState<Record<string, boolean>>({});
  const [error, setError] = useState("");
  const [discard, setDiscard] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const [modelToDelete, setModelToDelete] = useState<string | null>(null);
  const dirty = useRef(false);
  const formRef = useRef<HTMLFormElement>(null);
  const providerId = useRef(initial?.id ?? nextId(api.gateways.map(g => g.id), "gateway"));
  const sequence = useRef(0);
  function markDirty() { dirty.current = true; setError(""); }
  function close() { if (dirty.current) setDiscard(true); else onClose(); }
  function patchModel(id: string, patch: Partial<ModelDraft>) {
    markDirty();
    setModels(rows => rows.map(m => m.id === id ? { ...m, ...patch } : m));
  }
  function patchLevel(model: ModelDraft, i: number, patch: Partial<GatewayModelLevel>) {
    patchModel(model.id, { levels: model.levels.map((l, index) => index === i ? { ...l, ...patch } : l) });
  }
  function addModel() {
    markDirty();
    const id = nextId([...api.models.map(m => m.id), ...models.map(m => m.id), ...removedModels], `model-draft-${++sequence.current}`);
    setModels(rows => [...rows.map(m => ({ ...m, expanded: false })), { id, gatewayId: providerId.current, modelId: "", displayName: "", levels: [], vision: false, maxOutputTokens: null, expanded: true }]);
  }
  function removeModel() {
    if (!modelToDelete) return;
    markDirty();
    setRemovedModels(ids => [...ids, modelToDelete]);
    setModels(rows => rows.filter(m => m.id !== modelToDelete));
    setModelToDelete(null);
  }
  function modelError(model: ModelDraft, message: string, reasoning = false) {
    setModels(rows => rows.map(m => ({ ...m, expanded: m.id === model.id })));
    if (reasoning) setReasoningOpen(values => ({ ...values, [model.id]: true }));
    setError(message);
  }
  function submit(e?: FormEvent) {
    e?.preventDefault();
    if (!name.trim()) { setError("请输入供应商名称。"); return; }
    let parsed: URL;
    try { parsed = new URL(url.trim()); } catch { setError("请输入完整的 API 地址，例如 https://api.example.com/v1。"); return; }
    if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) { setError("API 地址须为 http(s) 基础地址，不能包含凭据、查询参数或片段。"); return; }
    const seen = new Set<string>();
    for (const m of models) {
      if (!MODEL_ID.test(m.modelId.trim())) { modelError(m, "模型 ID 需以字母或数字开头，支持 . _ : / -，最多 128 个字符。"); return; }
      if (seen.has(m.modelId.trim())) { modelError(m, "此供应商下的模型 ID 重复。"); return; }
      seen.add(m.modelId.trim());
      if (m.maxOutputTokens !== null && m.maxOutputTokens !== undefined && (!Number.isSafeInteger(m.maxOutputTokens) || m.maxOutputTokens <= 0)) { modelError(m, "最大输出 Token 需为正整数。"); return; }
      const levelIds = new Set<string>();
      for (const l of m.levels) {
        const message = !l.label.trim() ? "请填写推理档位名称。" : !LEVEL_ID.test(l.id.trim()) ? "档位标识支持字母、数字、点、下划线或连字符，最多 32 个字符。" : levelIds.has(l.id.trim()) ? "推理档位标识重复。" : l.effort.trim() && !EFFORT.test(l.effort.trim()) ? "effort 需以字母或数字开头，支持 . _ -，最多 64 个字符。" : protocol === "responses" && l.thinking !== "omit" ? "Responses 不支持 thinking，请改为「跟随模型」。" : "";
        if (message) { modelError(m, message, true); return; }
        levelIds.add(l.id.trim());
      }
    }
    const saved = api.saveGateway({ id: providerId.current, name: name.trim(), baseUrl: url.trim(), protocol, keySet: initial?.keySet ?? false }, clearKey ? null : key.trim() || undefined);
    if (!saved.ok) { setError(saved.error || "供应商保存失败。"); return; }
    for (const id of removedModels) if (api.models.some(m => m.id === id)) api.deleteModel(id);
    for (const { expanded: _, ...m } of models) {
      const result = api.saveModel({ ...m, modelId: m.modelId.trim(), displayName: m.displayName.trim() || m.modelId.trim() });
      if (!result.ok) { setError(result.error || "模型保存失败。"); return; }
    }
    onSaved();
  }
  const footer = deleteConfirm ? <><span className="mp-discard-message">删除供应商及其全部模型？</span><Button variant="outline" size="sm" onClick={() => setDeleteConfirm(false)}>保留</Button><Button variant="destructive" size="sm" onClick={onRemove}>确认删除</Button></> : discard ? <><span className="mp-discard-message">放弃未保存的更改？</span><Button variant="outline" size="sm" onClick={() => setDiscard(false)}>继续编辑</Button><Button variant="destructive" size="sm" onClick={onClose}>放弃更改</Button></> : <><div className="mp-foot-start">{initial && <Button type="button" variant="ghost" size="sm" className="mp-danger" onClick={() => setDeleteConfirm(true)}>删除供应商</Button>}</div><Button type="button" variant="outline" size="sm" onClick={close}>取消</Button><Button type="button" size="sm" onClick={() => formRef.current?.requestSubmit()}>保存</Button></>;
  return <Modal title={initial?.name ?? (preset?.id === "custom" || preset?.id === "openai" ? "自定义供应商" : preset?.name ?? "添加供应商")} className="mp-provider-dialog" onClose={close} footer={footer}>
    <form className="mp-editor" ref={formRef} onSubmit={submit}>
      <Field label="名称" id="gateway-name"><input id="gateway-name" autoFocus autoComplete="off" maxLength={128} placeholder="例如：团队网关" value={name} onChange={e => { setName(e.target.value); markDirty(); }}/></Field>
      <Field label="API 地址" id="gateway-url"><div className="mp-endpoint"><div className="mp-segmented" role="group" aria-label="API 协议">{PROTOCOLS.map(([id, text]) => <button type="button" key={id} aria-pressed={protocol === id} onClick={() => { setProtocol(id); markDirty(); }}>{text}</button>)}</div><input id="gateway-url" type="url" autoComplete="off" spellCheck={false} placeholder="https://api.example.com/v1" value={url} onChange={e => { setUrl(e.target.value); markDirty(); }}/></div></Field>
      <Field label="API Key" id="gateway-key" hint={clearKey ? "保存后清除密钥。" : initial?.keySet ? "已设置。留空保留，输入新密钥替换。" : "本地免密服务可留空。"}><div className="mp-key-input"><input id="gateway-key" type={showKey ? "text" : "password"} autoComplete="new-password" disabled={clearKey} value={key} placeholder={initial?.keySet ? "••••••••••••••••" : "输入 API Key"} onChange={e => { setKey(e.target.value); markDirty(); }}/><button type="button" className="mp-icon-button" aria-label={showKey ? "隐藏输入的密钥" : "显示输入的密钥"} onClick={() => setShowKey(!showKey)}>{showKey ? <EyeOff/> : <Eye/>}</button>{initial?.keySet && <button type="button" className="mp-text" onClick={() => { setClearKey(!clearKey); setKey(""); markDirty(); }}>{clearKey ? "保留" : "清除"}</button>}</div></Field>
      <Field label="模型"><div className="mp-model-box">
        {models.map(m => <div className="mp-model-entry" key={m.id}>
          <button type="button" className="mp-model-summary" aria-expanded={m.expanded} onClick={() => setModels(rows => rows.map(row => row.id === m.id ? { ...row, expanded: !row.expanded } : row))}><ChevronRight className={m.expanded ? "is-open" : ""} aria-hidden="true"/><span className="mp-model-title"><strong>{m.displayName || m.modelId || "新模型"}</strong>{m.modelId && <small>{m.modelId}</small>}</span>{api.selection.modelId === m.id && <span className="mp-tag">默认</span>}{m.vision && <span className="mp-capability">图片</span>}{m.levels.length > 0 && <span className="mp-capability">{m.levels.length} 档推理</span>}</button>
          {m.expanded && <div className="mp-model-options">
            <Field label="模型 ID" id={`model-id-${m.id}`}><input id={`model-id-${m.id}`} data-model-id value={m.modelId} maxLength={128} spellCheck={false} placeholder="例如：deepseek-chat" onChange={e => patchModel(m.id, { modelId: e.target.value })}/></Field>
            <Field label="显示名称" id={`model-name-${m.id}`}><input id={`model-name-${m.id}`} data-model-name value={m.displayName} maxLength={128} placeholder="留空使用模型 ID" onChange={e => patchModel(m.id, { displayName: e.target.value })}/></Field>
            <Field label="图片输入"><div className="mp-segmented" role="group" aria-label={`${m.displayName || "新模型"}图片输入`}>{[[false, "关闭"], [true, "支持"]].map(([v, text]) => <button type="button" key={String(v)} aria-pressed={m.vision === v} onClick={() => patchModel(m.id, { vision: v === true })}>{text}</button>)}</div></Field>
            <Field label="最大输出" id={`model-tokens-${m.id}`}><div className="mp-number-unit"><input id={`model-tokens-${m.id}`} data-model-tokens type="number" min={1} step={1} value={m.maxOutputTokens ?? ""} placeholder="模型默认" onChange={e => patchModel(m.id, { maxOutputTokens: e.target.value ? Number(e.target.value) : null })}/><span>Token</span></div></Field>
            <button type="button" className="mp-reasoning-toggle" aria-expanded={!!reasoningOpen[m.id]} onClick={() => setReasoningOpen(values => ({ ...values, [m.id]: !values[m.id] }))}><ChevronRight className={reasoningOpen[m.id] ? "is-open" : ""} aria-hidden="true"/>推理档位<span>{m.levels.length || "模型默认"}</span></button>
            {reasoningOpen[m.id] && <div className="mp-reasoning">
              {m.levels.length > 0 && <div className="mp-level-head" aria-hidden="true"><span>名称</span><span>标识</span><span>effort</span><span>thinking</span><span/></div>}
              {m.levels.map((l, i) => <div className="mp-level-row" key={i}>
                <input aria-label={`档位 ${i + 1} 名称`} placeholder="高" maxLength={32} value={l.label} onChange={e => patchLevel(m, i, { label: e.target.value })}/>
                <input aria-label={`档位 ${i + 1} 标识`} placeholder="high" maxLength={32} value={l.id} onChange={e => patchLevel(m, i, { id: e.target.value })}/>
                <input aria-label={`档位 ${i + 1} effort`} placeholder="不发送" list="mp-effort-values" maxLength={64} value={l.effort} onChange={e => patchLevel(m, i, { effort: e.target.value })}/>
                <Select label={`档位 ${i + 1} thinking`} value={l.thinking} options={THINKING} onChange={v => patchLevel(m, i, { thinking: v as GatewayThinking })}/>
                <button type="button" className="mp-icon-button" aria-label={`删除档位 ${l.label || i + 1}`} onClick={() => patchModel(m.id, { levels: m.levels.filter((_, index) => index !== i) })}><Trash2 aria-hidden="true"/></button>
              </div>)}
              <div className="mp-reasoning-actions"><button className="mp-text" type="button" onClick={() => patchModel(m.id, { levels: [...m.levels, { id: "", label: "", effort: "", thinking: "omit" }] })}><Plus aria-hidden="true"/>添加档位</button>{!m.levels.length && <button type="button" className="mp-text" onClick={() => patchModel(m.id, { levels: ([["low", "低"], ["medium", "中"], ["high", "高"], ["xhigh", "超高"]] as const).map(([id, label]) => ({ id, label, effort: id, thinking: "omit" })) })}>填入常用档位</button>}</div>
              <p className="mp-hint">effort 可自定义，留空不发送。{protocol === "responses" ? "Responses 的 thinking 须选「跟随模型」。" : "请按模型支持的参数填写。"}</p>
            </div>}
            <div className="mp-model-bottom"><button type="button" className="mp-text mp-danger" onClick={() => setModelToDelete(m.id)}><Trash2 aria-hidden="true"/>移除此模型</button></div>
            {modelToDelete === m.id && <div className="mp-inline-confirm"><span>移除此模型？保存后生效。</span><button type="button" className="mp-text" onClick={() => setModelToDelete(null)}>保留</button><button type="button" className="mp-text mp-danger" onClick={removeModel}>移除</button></div>}
          </div>}
        </div>)}
        {!models.length && <div className="mp-models-empty">还没有模型，添加后即可在聊天中选择。</div>}
        <button className="mp-model-add" type="button" onClick={addModel}><Plus aria-hidden="true"/>添加模型</button>
      </div></Field>
      <datalist id="mp-effort-values">{["minimal", "low", "medium", "high", "xhigh"].map(v => <option value={v} key={v}/>)}</datalist>
      {error && <p className="mp-error" role="alert">{error}</p>}
    </form>
  </Modal>;
}

function GatewaySettingsContent({ onBack }: GatewaySettingsProps) {
  const api = useGatewayPreview();
  const [search, setSearch] = useState("");
  const [addOpen, setAddOpen] = useState(false);
  const [editor, setEditor] = useState<{ initial: Gateway | null; preset?: Preset } | null>(null);
  const [toast, setToast] = useState("");
  const toastTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const defaultModel = api.models.find(m => m.id === api.selection.modelId);
  const matches = api.gateways.filter(g => `${g.name} ${g.baseUrl} ${api.models.filter(m => m.gatewayId === g.id).map(m => `${m.modelId} ${m.displayName}`).join(" ")}`.toLowerCase().includes(search.trim().toLowerCase()));
  function notify(message: string) { clearTimeout(toastTimer.current); setToast(message); toastTimer.current = setTimeout(() => setToast(""), 2800); }
  function removeProvider() {
    const gateway = editor?.initial;
    if (!gateway) return;
    for (const m of api.models.filter(m => m.gatewayId === gateway.id)) api.deleteModel(m.id);
    const result = api.deleteGateway(gateway.id);
    if (result.ok) { setEditor(null); notify("供应商已删除"); }
  }
  return <div className="mp-gateways">
    {onBack && <Button variant="ghost" size="sm" onClick={onBack}>返回</Button>}
    <header className="mp-page-head"><h2>接入与模型</h2><label className="mp-search"><Search aria-hidden="true"/><input aria-label="搜索供应商或模型" placeholder="搜索供应商或模型…" value={search} onChange={e => setSearch(e.target.value)}/></label></header>
    {api.statusMessage && <div className="mp-banner" role="alert">{api.statusMessage}<button className="mp-text" onClick={api.clearStatus}>关闭</button></div>}
    <div className="mp-provider-list">{matches.map(g => {
      const models = api.models.filter(m => m.gatewayId === g.id);
      const currentDefault = api.selection.modelId && models.some(m => m.id === api.selection.modelId);
      return <button type="button" className="mp-provider-row" key={g.id} onClick={() => setEditor({ initial: g })}>
        <ProviderIcon url={g.baseUrl}/><span className="mp-provider-name"><strong>{g.name}</strong><small>{hostOf(g.baseUrl)} · {models.length} 个模型</small></span>
        {currentDefault && <span className="mp-default-label">默认</span>}
        <span className={`mp-key-state${g.keySet ? " is-set" : ""}`} aria-label={g.keySet ? "密钥已设置" : "未设置密钥"} title={g.keySet ? "密钥已设置" : "未设置密钥"}><span/>{g.keySet ? "密钥已设置" : "未设置密钥"}</span><ChevronRight aria-hidden="true"/>
      </button>;
    })}{!matches.length && <div className="mp-list-empty">{api.gateways.length ? "没有匹配的供应商或模型" : "还没有供应商"}</div>}</div>
    <button type="button" className="mp-add-provider" onClick={() => setAddOpen(true)}><Plus aria-hidden="true"/>添加供应商</button>
    <section className="mp-default-preference" aria-label="新对话默认模型"><div><h3>新对话默认</h3><p>已有对话保留各自的选择。</p></div><div className="mp-default-controls"><Select label="默认模型" value={api.selection.modelId ?? ""} options={[["", "未选择模型"], ...api.models.map(m => [m.id, `${m.displayName} · ${api.gateways.find(g => g.id === m.gatewayId)?.name ?? ""}`] as const)]} onChange={v => api.selectModel(v || null)}/><Select label="默认推理档位" value={api.selection.levelId ?? ""} disabled={!defaultModel} options={[["", "模型默认"], ...(defaultModel?.levels.map(l => [l.id, l.label] as const) ?? [])]} onChange={v => api.selectLevel(v || null)}/></div></section>
    {addOpen && <AddProvider onClose={() => setAddOpen(false)} onChoose={preset => { setAddOpen(false); setEditor({ initial: null, preset }); }}/>}
    {editor && <ProviderEditor initial={editor.initial} preset={editor.preset} onClose={() => setEditor(null)} onSaved={() => { setEditor(null); notify("供应商已保存"); }} onRemove={removeProvider}/>}
    {toast && <div className="mp-toast" role="status"><Check aria-hidden="true"/>{toast}</div>}
  </div>;
}
export function GatewaySettings(props: GatewaySettingsProps) {
  const existing = useOptionalGatewayPreview();
  return existing ? <GatewaySettingsContent {...props}/> : <GatewayPreviewProvider><GatewaySettingsContent {...props}/></GatewayPreviewProvider>;
}
