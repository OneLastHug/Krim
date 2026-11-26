import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { Check, ChevronDown, ChevronRight, Cpu, Download, Eye, EyeOff, Network, Plus, Search, Server, Trash2, X } from "lucide-react";
import { useModelGateways, type ModelGatewayContext } from "../hooks/use-model-gateways";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogTitle } from "./ui/dialog";
import { gatewayDiscoveryError, type Gateway, type GatewayModel, type GatewayModelLevel, type GatewayProtocol, type GatewayThinking } from "../lib/model-gateways";
import { appendReasoningPreset, OUTPUT_TOKEN_PRESETS, PROTOCOL_HINTS, REASONING_EFFORTS, REASONING_PRESETS, reasoningWirePreview } from "../lib/model-gateway-presets";
import "./model-gateway-settings.css";

export interface ModelGatewaySettingsProps {}

type Preset = { id: string; name: string; hint: string; url: string; protocol: GatewayProtocol; group: string; icon: typeof Network };
const PRESETS: Preset[] = [
  { id: "deepseek", name: "DeepSeek", hint: "api.deepseek.com", url: "https://api.deepseek.com/v1", protocol: "chat_completions", group: "API 服务", icon: Cpu },
  { id: "openai", name: "OpenAI 兼容", hint: "官方 API 或中转", url: "", protocol: "chat_completions", group: "API 服务", icon: Network },
  { id: "ollama", name: "Ollama", hint: "localhost:11434", url: "http://localhost:11434/v1", protocol: "chat_completions", group: "本地与自定义", icon: Server },
  { id: "lmstudio", name: "LM Studio", hint: "localhost:1234", url: "http://localhost:1234/v1", protocol: "chat_completions", group: "本地与自定义", icon: Server },
  { id: "custom", name: "自定义", hint: "任意兼容的 API 地址", url: "", protocol: "chat_completions", group: "本地与自定义", icon: Plus },
];
const MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;
const GATEWAY_ID = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const LEVEL_ID = /^[A-Za-z0-9._-]{1,32}$/;
const EFFORT = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/;
const PROTOCOLS = [["chat_completions", "Chat Completions"], ["responses", "Responses"]] as const;
const THINKING = [["omit", "跟随模型"], ["enabled", "开启"], ["disabled", "关闭"]] as const;

function nextGatewayId(ids: string[]): string {
  let n = 1;
  while (ids.includes(`gateway-${n}`)) n++;
  return `gateway-${n}`;
}

function hostOf(url: string) {
  try { return new URL(url).host; } catch { return url; }
}

function ProviderIcon({ url = "" }: { url?: string }) {
  const local = /^https?:\/\/(localhost|127\.0\.0\.1|\[::1\])(?=[:/]|$)/.test(url);
  const Icon = local ? Server : /deepseek\.com/.test(url) ? Cpu : Network;
  return <Icon className="mp-provider-icon" aria-hidden="true" />;
}

function Select({ label, value, options, onChange, disabled }: {
  label: string;
  value: string;
  options: readonly (readonly [string, string])[];
  onChange: (value: string) => void;
  disabled?: boolean;
}) {
  return (
    <span className="mp-select">
      <select aria-label={label} value={value} disabled={disabled} onChange={event => onChange(event.target.value)}>
        {options.map(([optionValue, text]) => <option key={optionValue} value={optionValue}>{text}</option>)}
      </select>
      <ChevronDown aria-hidden="true" />
    </span>
  );
}

function Field({ label, id, hint, children }: { label: string; id?: string; hint?: string; children: ReactNode }) {
  return (
    <div className="mp-field">
      <label htmlFor={id}>{label}</label>
      <div className="mp-field-control">
        {children}
        {hint && <p className="mp-hint">{hint}</p>}
      </div>
    </div>
  );
}

function Modal({ title, description, onClose, footer, children, className = "", busy = false }: {
  title: string;
  description?: string;
  onClose: () => void;
  footer?: ReactNode;
  children?: ReactNode;
  className?: string;
  busy?: boolean;
}) {
  return (
    <Dialog open onOpenChange={open => { if (!open && !busy) onClose(); }}>
      <DialogContent showCloseButton={false} className={`mp-dialog ${className}`}>
        <header className="mp-dialog-head">
          <DialogTitle>{title}</DialogTitle>
          {description && <DialogDescription>{description}</DialogDescription>}
          <button type="button" className="mp-dialog-close" aria-label="关闭" disabled={busy} onClick={onClose}>
            <X aria-hidden="true" />
          </button>
        </header>
        {children && <div className="mp-dialog-body">{children}</div>}
        {footer && <DialogFooter className="mp-dialog-foot">{footer}</DialogFooter>}
      </DialogContent>
    </Dialog>
  );
}

function AddProvider({ onChoose, onClose }: { onChoose: (preset: Preset) => void; onClose: () => void }) {
  const [query, setQuery] = useState("");
  const matches = PRESETS.filter(preset => `${preset.name} ${preset.hint}`.toLowerCase().includes(query.trim().toLowerCase()));
  return (
    <Modal title="添加供应商" className="mp-add-dialog" onClose={onClose}>
      <label className="mp-search mp-add-search">
        <Search aria-hidden="true" />
        <input autoFocus placeholder="查找供应商…" aria-label="查找供应商" value={query} onChange={event => setQuery(event.target.value)} />
      </label>
      {["API 服务", "本地与自定义"].map(group => {
        const items = matches.filter(preset => preset.group === group);
        return items.length > 0 && (
          <section className="mp-preset-group" key={group}>
            <h3>{group}</h3>
            <div className="mp-presets">
              {items.map(preset => (
                <button type="button" key={preset.id} className={preset.id === "custom" ? "mp-custom-preset" : undefined} onClick={() => onChoose(preset)}>
                  <preset.icon aria-hidden="true" />
                  <span><strong>{preset.name}</strong><small>{preset.hint}</small></span>
                  <ChevronRight aria-hidden="true" />
                </button>
              ))}
            </div>
          </section>
        );
      })}
      {!matches.length && <p className="mp-no-results">没有匹配的供应商。可以清空搜索后选择「自定义」。</p>}
    </Modal>
  );
}

type ModelDraft = GatewayModel & { expanded: boolean };
type ProviderEditorProps = {
  api: ModelGatewayContext;
  initial: Gateway | null;
  preset?: Preset;
  onClose: () => void;
  onSaved: () => void;
  onReloaded: () => void;
  onRemove: () => Promise<void>;
};

function isUnknownMessage(message: string) {
  return message.includes("无法确认保存结果") || message.includes("结果尚未确认") || message.includes("配置已被其他操作更新");
}

function ProviderEditor({ api, initial, preset, onClose, onSaved, onReloaded, onRemove }: ProviderEditorProps) {
  const [name, setName] = useState(initial?.name ?? (preset?.id === "custom" || preset?.id === "openai" ? "" : preset?.name ?? ""));
  const [url, setUrl] = useState(initial?.baseUrl ?? preset?.url ?? "");
  const [protocol, setProtocol] = useState<GatewayProtocol>(initial?.protocol ?? preset?.protocol ?? "chat_completions");
  const [key, setKey] = useState("");
  const [showKey, setShowKey] = useState(false);
  const [clearKey, setClearKey] = useState(false);
  const [models, setModels] = useState<ModelDraft[]>(() => api.models.filter(model => model.gatewayId === initial?.id).map(model => ({ ...model, levels: model.levels.map(level => ({ ...level })), expanded: false })));
  const [discovery, setDiscovery] = useState<{ loading: boolean; models: GatewayModel[] | null; selected: Set<string>; query: string; error: string }>({ loading: false, models: null, selected: new Set(), query: "", error: "" });
  const discoveryTicket = useRef(0);
  const editorLive = useRef(true);
  useEffect(() => { editorLive.current = true; return () => { editorLive.current = false; discoveryTicket.current++; }; }, []);
  const [reasoningOpen, setReasoningOpen] = useState<Record<string, boolean>>({});
  const [reasoningPreset, setReasoningPreset] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  const [needsReload, setNeedsReload] = useState(false);
  const [reloadBusy, setReloadBusy] = useState(false);
  const [discard, setDiscard] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [modelToDelete, setModelToDelete] = useState<string | null>(null);
  const dirty = useRef(false);
  const formRef = useRef<HTMLFormElement>(null);
  const providerId = useRef(initial?.id ?? nextGatewayId(api.gateways.map(gateway => gateway.id)));
  const sequence = useRef(0);
  const controlsBusy = api.loading || api.saving || deleteBusy;
  const formBusy = controlsBusy || discovery.loading || !api.canWrite;
  const existingModelIds = new Set(models.map(model => model.modelId.trim()));
  const discoveredMatches = (discovery.models ?? []).filter(model => `${model.modelId} ${model.displayName}`.toLowerCase().includes(discovery.query.trim().toLowerCase()));
  const availableMatches = discoveredMatches.filter(model => !existingModelIds.has(model.modelId));
  const selectedDiscoveries = (discovery.models ?? []).filter(model => discovery.selected.has(model.modelId) && !existingModelIds.has(model.modelId));

  function invalidateDiscovery() {
    discoveryTicket.current++;
    setDiscovery({ loading: false, models: null, selected: new Set(), query: "", error: "" });
  }

  async function discoverModels() {
    if (formBusy || needsReload) return;
    const ticket = ++discoveryTicket.current;
    setDiscovery({ loading: true, models: null, selected: new Set(), query: "", error: "" });
    try {
      const result = await api.discoverModels({ gatewayId: initial?.id, baseUrl: url, protocol, apiKeyEnv: initial?.apiKeyEnv, keyDraft: clearKey ? null : key.trim() || undefined });
      if (!editorLive.current || ticket !== discoveryTicket.current) return;
      setDiscovery({ loading: false, models: result, selected: new Set(), query: "", error: "" });
    } catch (caught) {
      if (!editorLive.current || ticket !== discoveryTicket.current) return;
      const message = gatewayDiscoveryError(caught);
      setDiscovery({ loading: false, models: null, selected: new Set(), query: "", error: message });
      if (isUnknownMessage(message)) { setError(message); setNeedsReload(true); }
    }
  }

  function addDiscovered() {
    if (formBusy || !selectedDiscoveries.length) return;
    markDirty();
    setModels(rows => {
      const ids = new Set(rows.map(model => model.modelId.trim()));
      return [...rows, ...selectedDiscoveries.filter(model => !ids.has(model.modelId)).map(model => ({ ...model, id: `model-draft-${++sequence.current}`, gatewayId: providerId.current, levels: model.levels.map(level => ({ ...level })), expanded: false }))];
    });
    setDiscovery(previous => ({ ...previous, selected: new Set() }));
  }

  function markDirty() {
    dirty.current = true;
    setError("");
  }

  function close() {
    if (controlsBusy) return;
    if (dirty.current) setDiscard(true);
    else onClose();
  }

  function patchModel(id: string, patch: Partial<ModelDraft>) {
    markDirty();
    setModels(rows => rows.map(model => model.id === id ? { ...model, ...patch } : model));
  }

  function patchLevel(model: ModelDraft, index: number, patch: Partial<GatewayModelLevel>) {
    patchModel(model.id, { levels: model.levels.map((level, levelIndex) => levelIndex === index ? { ...level, ...patch } : level) });
  }

  function addModel() {
    if (controlsBusy || !api.canWrite) return;
    markDirty();
    const id = `model-draft-${++sequence.current}`;
    setModels(rows => [
      ...rows.map(model => ({ ...model, expanded: false })),
      { id, gatewayId: providerId.current, modelId: "", displayName: "", levels: [], vision: false, maxOutputTokens: null, expanded: true },
    ]);
  }

  function removeModel() {
    if (!modelToDelete || controlsBusy || !api.canWrite) return;
    markDirty();
    setModels(rows => rows.filter(model => model.id !== modelToDelete));
    setModelToDelete(null);
  }

  function modelError(model: ModelDraft, message: string, reasoning = false) {
    setModels(rows => rows.map(row => ({ ...row, expanded: row.id === model.id })));
    if (reasoning) setReasoningOpen(values => ({ ...values, [model.id]: true }));
    setError(message);
  }

  async function reloadDraft() {
    if (reloadBusy || controlsBusy) return;
    setReloadBusy(true);
    try {
      await api.reload();
      onReloaded();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "无法重新读取模型配置。");
    } finally {
      setReloadBusy(false);
    }
  }

  async function submit(event?: FormEvent) {
    event?.preventDefault();
    if (formBusy || needsReload) return;
    if (!name.trim()) { setError("请输入供应商名称。"); return; }
    if (!GATEWAY_ID.test(providerId.current)) { setError("供应商标识无效，请重新读取配置后重试。"); return; }
    let parsed: URL;
    try { parsed = new URL(url.trim()); } catch { setError("请输入完整的 API 地址，例如 https://api.example.com/v1。"); return; }
    if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) {
      setError("API 地址须为 http(s) 基础地址，不能包含凭据、查询参数或片段。");
      return;
    }
    const seen = new Set<string>();
    for (const model of models) {
      const wireModelId = model.modelId.trim();
      if (!MODEL_ID.test(wireModelId)) { modelError(model, "模型 ID 需以字母或数字开头，支持 . _ : / -，最多 128 个字符。"); return; }
      if (seen.has(wireModelId)) { modelError(model, "此供应商下的模型 ID 重复。"); return; }
      seen.add(wireModelId);
      if (model.maxOutputTokens !== null && model.maxOutputTokens !== undefined && (!Number.isSafeInteger(model.maxOutputTokens) || model.maxOutputTokens <= 0)) {
        modelError(model, "最大输出 Token 需为正整数。");
        return;
      }
      const levelIds = new Set<string>();
      for (const level of model.levels) {
        const levelError = !level.label.trim()
          ? "请填写推理档位名称。"
          : !LEVEL_ID.test(level.id.trim())
            ? "档位标识支持字母、数字、点、下划线或连字符，最多 32 个字符。"
            : levelIds.has(level.id.trim())
              ? "推理档位标识重复。"
              : level.effort.trim() && !EFFORT.test(level.effort.trim())
                ? "effort 需以字母或数字开头，支持 . _ -，最多 64 个字符。"
                : protocol === "responses" && level.thinking !== "omit"
                  ? "Responses 不支持 thinking，请改为「跟随模型」。"
                  : "";
        if (levelError) { modelError(model, levelError, true); return; }
        levelIds.add(level.id.trim());
      }
    }
    const gateway: Gateway = {
      id: providerId.current,
      name: name.trim(),
      baseUrl: url.trim(),
      protocol,
      keySet: initial?.keySet ?? false,
      apiKeyEnv: initial?.apiKeyEnv ?? null,
    };
    const normalizedModels = models.map(({ expanded: _expanded, ...model }) => ({
      ...model,
      gatewayId: gateway.id,
      modelId: model.modelId.trim(),
      displayName: model.displayName.trim() || model.modelId.trim(),
      levels: model.levels.map(level => ({ ...level, id: level.id.trim(), label: level.label.trim(), effort: level.effort.trim() })),
    }));
    try {
      await api.saveProvider(gateway, normalizedModels, clearKey ? null : key.trim() || undefined);
      onSaved();
    } catch (caught) {
      const message = caught instanceof Error ? caught.message : "供应商保存失败。";
      setError(message);
      if (isUnknownMessage(message)) setNeedsReload(true);
    }
  }

  async function remove() {
    if (!initial || deleteBusy || controlsBusy || !api.canWrite || needsReload) return;
    setDeleteBusy(true);
    setError("");
    try {
      await onRemove();
    } catch (caught) {
      const message = caught instanceof Error ? caught.message : "供应商删除失败。";
      setError(message);
      if (isUnknownMessage(message)) setNeedsReload(true);
      setDeleteBusy(false);
    }
  }

  const title = initial?.name ?? (preset?.id === "custom" || preset?.id === "openai" ? "自定义供应商" : preset?.name ?? "添加供应商");
  const footer = deleteConfirm ? (
    <>
      <span className="mp-discard-message">删除供应商及其全部模型？</span>
      <Button variant="outline" size="sm" disabled={deleteBusy} onClick={() => setDeleteConfirm(false)}>保留</Button>
      <Button variant="destructive" size="sm" disabled={deleteBusy || formBusy || needsReload} onClick={() => void remove()}>{deleteBusy ? "正在删除…" : "确认删除"}</Button>
    </>
  ) : discard ? (
    <>
      <span className="mp-discard-message">放弃未保存的更改？</span>
      <Button variant="outline" size="sm" disabled={formBusy} onClick={() => setDiscard(false)}>继续编辑</Button>
      <Button variant="destructive" size="sm" disabled={formBusy} onClick={onClose}>放弃更改</Button>
    </>
  ) : (
    <>
      <div className="mp-foot-start">
        {initial && <Button type="button" variant="ghost" size="sm" className="mp-danger" disabled={formBusy || needsReload} onClick={() => setDeleteConfirm(true)}>删除供应商</Button>}
      </div>
      <Button type="button" variant="outline" size="sm" disabled={controlsBusy} onClick={close}>取消</Button>
      <Button type="button" size="sm" disabled={formBusy || needsReload} onClick={() => formRef.current?.requestSubmit()}>{api.saving ? "正在保存…" : "保存"}</Button>
    </>
  );

  return (
    <Modal title={title} className="mp-provider-dialog" onClose={close} busy={controlsBusy} footer={footer}>
      <form className="mp-editor" ref={formRef} onSubmit={event => void submit(event)}>
        <Field label="名称" id="gateway-name">
          <input id="gateway-name" autoFocus autoComplete="off" maxLength={128} placeholder="例如：团队网关" value={name} disabled={formBusy} onChange={event => { setName(event.target.value); markDirty(); }} />
        </Field>
        <Field label="API 地址" id="gateway-url" hint={PROTOCOL_HINTS[protocol]}>
          <div className="mp-endpoint">
            <div className="mp-segmented" role="group" aria-label="API 协议">
              {PROTOCOLS.map(([protocolId, text]) => (
                <button type="button" key={protocolId} aria-pressed={protocol === protocolId} disabled={formBusy} onClick={() => { setProtocol(protocolId); invalidateDiscovery(); markDirty(); }}>{text}</button>
              ))}
            </div>
            <input id="gateway-url" type="url" autoComplete="off" spellCheck={false} placeholder="https://api.example.com/v1" value={url} disabled={formBusy} onChange={event => { setUrl(event.target.value); invalidateDiscovery(); markDirty(); }} />
          </div>
        </Field>
        <Field label="API Key" id="gateway-key" hint={clearKey ? "保存后清除密钥。" : initial?.keySet ? "已设置。留空保留，输入新密钥替换。" : "本地免密服务可留空。"}>
          <div className="mp-key-input">
            <input id="gateway-key" type={showKey ? "text" : "password"} autoComplete="new-password" disabled={formBusy || clearKey} value={key} placeholder={initial?.keySet ? "••••••••••••••••" : "输入 API Key"} onChange={event => { setKey(event.target.value); invalidateDiscovery(); markDirty(); }} />
            <button type="button" className="mp-icon-button" aria-label={showKey ? "隐藏输入的密钥" : "显示输入的密钥"} disabled={formBusy || clearKey} onClick={() => setShowKey(value => !value)}>{showKey ? <EyeOff /> : <Eye />}</button>
            {initial?.keySet && <button type="button" className="mp-text" disabled={formBusy} onClick={() => { setClearKey(value => !value); setKey(""); invalidateDiscovery(); markDirty(); }}>{clearKey ? "保留" : "清除"}</button>}
          </div>
        </Field>
        <Field label="模型">
          <div className="mp-model-box">
            {models.map(model => (
              <div className="mp-model-entry" key={model.id}>
                <button type="button" className="mp-model-summary" aria-expanded={model.expanded} disabled={formBusy} onClick={() => setModels(rows => rows.map(row => row.id === model.id ? { ...row, expanded: !row.expanded } : row))}>
                  <ChevronRight className={model.expanded ? "is-open" : ""} aria-hidden="true" />
                  <span className="mp-model-title"><strong>{model.displayName || model.modelId || "新模型"}</strong>{model.modelId && <small>{model.modelId}</small>}</span>
                  {api.selection.modelId === model.id && <span className="mp-tag">默认</span>}
                  {model.vision && <span className="mp-capability">图片</span>}
                  {model.levels.length > 0 && <span className="mp-capability">{model.levels.length} 档推理</span>}
                </button>
                {model.expanded && (
                  <div className="mp-model-options">
                    <Field label="模型 ID" id={`model-id-${model.id}`}><input id={`model-id-${model.id}`} data-model-id value={model.modelId} maxLength={128} spellCheck={false} placeholder="例如：deepseek-chat" disabled={formBusy} onChange={event => patchModel(model.id, { modelId: event.target.value })} /></Field>
                    <Field label="显示名称" id={`model-name-${model.id}`}><input id={`model-name-${model.id}`} data-model-name value={model.displayName} maxLength={128} placeholder="留空使用模型 ID" disabled={formBusy} onChange={event => patchModel(model.id, { displayName: event.target.value })} /></Field>
                    <Field label="图片输入">
                      <div className="mp-segmented" role="group" aria-label={`${model.displayName || "新模型"}图片输入`}>
                        {[[false, "关闭"], [true, "支持"]].map(([value, text]) => <button type="button" key={String(value)} aria-pressed={model.vision === value} disabled={formBusy} onClick={() => patchModel(model.id, { vision: value === true })}>{text}</button>)}
                      </div>
                    </Field>
                    <Field label="最大输出" id={`model-tokens-${model.id}`}>
                      <div className="mp-number-unit"><input id={`model-tokens-${model.id}`} data-model-tokens type="number" min={1} step={1} value={model.maxOutputTokens ?? ""} placeholder="模型默认" disabled={formBusy} onChange={event => patchModel(model.id, { maxOutputTokens: event.target.value ? Number(event.target.value) : null })} /><span>Token</span></div>
                      <div className="mp-quick-picks" role="group" aria-label="最大输出快捷档位">
                        <button type="button" aria-pressed={model.maxOutputTokens == null} disabled={formBusy} onClick={() => patchModel(model.id, { maxOutputTokens: null })}>模型默认</button>
                        {OUTPUT_TOKEN_PRESETS.map(value => <button key={value} type="button" aria-pressed={model.maxOutputTokens === value} disabled={formBusy} onClick={() => patchModel(model.id, { maxOutputTokens: value })}>{value >= 1000000 ? `${value / 1000000}M` : `${value / 1000}K`}</button>)}
                      </div>
                      <p className="mp-hint">限制单次回复的输出量；留空由端点决定。此值不同于上下文窗口容量。</p>
                    </Field>
                    <button type="button" className="mp-reasoning-toggle" aria-expanded={!!reasoningOpen[model.id]} disabled={formBusy} onClick={() => setReasoningOpen(values => ({ ...values, [model.id]: !values[model.id] }))}><ChevronRight className={reasoningOpen[model.id] ? "is-open" : ""} aria-hidden="true" />推理档位<span>{model.levels.length || "模型默认"}</span></button>
                    {reasoningOpen[model.id] && (
                      <div className="mp-reasoning">
                        <div className="mp-reasoning-presets">
                          <Select label="推理参数预设" value={REASONING_PRESETS.find(item => item.id === reasoningPreset[model.id] && (protocol === "chat_completions" || !item.chatOnly))?.id ?? "common"} options={REASONING_PRESETS.filter(preset => protocol === "chat_completions" || !preset.chatOnly).map(preset => [preset.id, preset.name] as const)} disabled={formBusy} onChange={value => setReasoningPreset(previous => ({ ...previous, [model.id]: value }))} />
                          <button type="button" className="mp-text" disabled={formBusy} onClick={() => { const preset = REASONING_PRESETS.find(item => item.id === reasoningPreset[model.id] && (protocol === "chat_completions" || !item.chatOnly)) ?? REASONING_PRESETS[0]!; patchModel(model.id, { levels: appendReasoningPreset(model.levels, preset) }); }}>添加预设档位</button>
                        </div>
                        <p className="mp-hint">{(REASONING_PRESETS.find(item => item.id === reasoningPreset[model.id] && (protocol === "chat_completions" || !item.chatOnly)) ?? REASONING_PRESETS[0]!).hint} 按端点支持选择，可继续修改。</p>
                        {model.levels.length > 0 && <div className="mp-level-head" aria-hidden="true"><span>名称</span><span>标识</span><span>effort</span><span>thinking</span><span /></div>}
                        {model.levels.map((level, index) => (
                          <div className="mp-level-entry" key={`${model.id}-${index}`}>
                          <div className="mp-level-row">
                            <input aria-label={`档位 ${index + 1} 名称`} placeholder="高" maxLength={32} value={level.label} disabled={formBusy} onChange={event => patchLevel(model, index, { label: event.target.value })} />
                            <input aria-label={`档位 ${index + 1} 标识`} placeholder="high" maxLength={32} value={level.id} disabled={formBusy} onChange={event => patchLevel(model, index, { id: event.target.value })} />
                            <input aria-label={`档位 ${index + 1} effort`} placeholder="不发送" list="mp-effort-values" maxLength={64} value={level.effort} disabled={formBusy} onChange={event => patchLevel(model, index, { effort: event.target.value })} />
                            <Select label={`档位 ${index + 1} thinking`} value={level.thinking} options={THINKING} disabled={formBusy} onChange={value => patchLevel(model, index, { thinking: value as GatewayThinking })} />
                            <button type="button" className="mp-icon-button" aria-label={`删除档位 ${level.label || index + 1}`} disabled={formBusy} onClick={() => patchModel(model.id, { levels: model.levels.filter((_, levelIndex) => levelIndex !== index) })}><Trash2 aria-hidden="true" /></button>
                          </div>
                          <code className="mp-level-preview" aria-label={`档位 ${index + 1} 请求参数`}>{reasoningWirePreview(level, protocol)}</code>
                          </div>
                        ))}
                        <div className="mp-reasoning-actions">
                          <button className="mp-text" type="button" disabled={formBusy} onClick={() => patchModel(model.id, { levels: [...model.levels, { id: "", label: "", effort: "", thinking: "omit" }] })}><Plus aria-hidden="true" />添加档位</button>
                        </div>
                        <p className="mp-hint">effort 可选 none、minimal、low、medium、high、xhigh、max，也可自定义；留空不发送。none 会明确发送关闭强度，与留空不同。</p>
                        <p className="mp-hint">{protocol === "responses" ? "Responses 发送 reasoning.effort；thinking 须选「跟随模型」。" : "Chat Completions 发送 reasoning_effort；thinking 发送 type: enabled / disabled，跟随模型则省略该字段。"}</p>
                      </div>
                    )}
                    <div className="mp-model-bottom"><button type="button" className="mp-text mp-danger" disabled={formBusy} onClick={() => setModelToDelete(model.id)}><Trash2 aria-hidden="true" />移除此模型</button></div>
                    {modelToDelete === model.id && <div className="mp-inline-confirm"><span>移除此模型？保存后生效。</span><button type="button" className="mp-text" disabled={formBusy} onClick={() => setModelToDelete(null)}>保留</button><button type="button" className="mp-text mp-danger" disabled={formBusy} onClick={removeModel}>移除</button></div>}
                  </div>
                )}
              </div>
            ))}
            {!models.length && <div className="mp-models-empty">还没有模型，添加后即可在聊天中选择。</div>}
            <div className="mp-model-actions">
              <button className="mp-model-add" type="button" disabled={formBusy} onClick={addModel}><Plus aria-hidden="true" />添加模型</button>
              <button className="mp-model-add" type="button" disabled={formBusy || needsReload || !url.trim()} onClick={() => void discoverModels()}><Download aria-hidden="true" />{discovery.loading ? "正在获取…" : "获取端点模型"}</button>
            </div>
          </div>
          {discovery.error && <p className="mp-discovery-error" role="alert">{discovery.error}</p>}
          {discovery.loading && <p className="mp-hint" role="status">正在请求端点的 /models 列表…</p>}
          {discovery.models !== null && <section className="mp-discovery" aria-label="端点模型列表">
            <div className="mp-discovery-head"><strong>端点模型 <span>{discovery.models.length}</span></strong><button type="button" className="mp-icon-button" aria-label="收起端点模型列表" onClick={invalidateDiscovery}><X aria-hidden="true" /></button></div>
            <label className="mp-search mp-discovery-search"><Search aria-hidden="true" /><input placeholder="搜索模型 ID 或名称…" aria-label="搜索端点模型" value={discovery.query} onChange={event => setDiscovery(previous => ({ ...previous, query: event.target.value }))} /></label>
            <div className="mp-discovery-selection">
              <button type="button" className="mp-text" disabled={formBusy || !availableMatches.length} onClick={() => setDiscovery(previous => ({ ...previous, selected: new Set([...previous.selected, ...availableMatches.map(model => model.modelId)]) }))}>选择搜索结果</button>
              <button type="button" className="mp-text" disabled={formBusy || !discovery.selected.size} onClick={() => setDiscovery(previous => ({ ...previous, selected: new Set() }))}>清空选择</button>
              <span>已选 {selectedDiscoveries.length} 个</span>
            </div>
            <div className="mp-discovery-list">
              {discoveredMatches.map(model => {
                const exists = existingModelIds.has(model.modelId);
                return <label className={`mp-discovery-row${exists ? " is-added" : ""}`} key={model.modelId}>
                  <input type="checkbox" aria-label={`选择模型 ${model.modelId}`} checked={exists || discovery.selected.has(model.modelId)} disabled={formBusy || exists} onChange={event => { const checked = event.target.checked; setDiscovery(previous => { const selected = new Set(previous.selected); if (checked) selected.add(model.modelId); else selected.delete(model.modelId); return { ...previous, selected }; }); }} />
                  <span><strong>{model.displayName}</strong>{model.displayName !== model.modelId && <small>{model.modelId}</small>}</span>
                  {exists && <small>已添加</small>}
                </label>;
              })}
              {!discoveredMatches.length && <p className="mp-models-empty">{discovery.models.length ? "没有匹配的模型" : "端点未返回模型，可手动添加模型 ID。"}</p>}
            </div>
            <div className="mp-discovery-foot"><span>添加后可设置能力和推理参数，保存后生效。</span><Button type="button" size="sm" disabled={formBusy || !selectedDiscoveries.length} onClick={addDiscovered}>添加所选{selectedDiscoveries.length ? `（${selectedDiscoveries.length}）` : ""}</Button></div>
          </section>}
        </Field>
        <datalist id="mp-effort-values">{REASONING_EFFORTS.map(([value, label]) => <option value={value} label={label} key={value} />)}</datalist>
        {needsReload && (
          <div className="mp-banner mp-editor-notice" role="alert">
            <span>保存结果尚未确认，当前草稿已保留。请先重新读取配置核对，不会自动换密钥重发。</span>
            <Button variant="outline" size="sm" type="button" disabled={reloadBusy || controlsBusy} onClick={() => void reloadDraft()}>{reloadBusy ? "正在读取…" : "重新读取配置"}</Button>
          </div>
        )}
        {error && <p className="mp-error" role="alert">{error}</p>}
      </form>
    </Modal>
  );
}

function GatewaySettingsContent({ api }: { api: ModelGatewayContext }) {
  const [search, setSearch] = useState("");
  const [addOpen, setAddOpen] = useState(false);
  const [editor, setEditor] = useState<{ initial: Gateway | null; preset?: Preset } | null>(null);
  const [toast, setToast] = useState("");
  const [reloadBusy, setReloadBusy] = useState(false);
  const [defaultBusy, setDefaultBusy] = useState(false);
  const [defaultError, setDefaultError] = useState("");
  const toastTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const readOnly = api.loading || api.saving || !api.canWrite;
  const defaultModel = api.models.find(model => model.id === api.selection.modelId);
  const keyword = search.trim().toLowerCase();
  const matches = api.gateways.filter(gateway => `${gateway.name} ${gateway.baseUrl} ${api.models.filter(model => model.gatewayId === gateway.id).map(model => `${model.modelId} ${model.displayName}`).join(" ")}`.toLowerCase().includes(keyword));

  useEffect(() => () => clearTimeout(toastTimer.current), []);

  function notify(message: string) {
    clearTimeout(toastTimer.current);
    setToast(message);
    toastTimer.current = setTimeout(() => setToast(""), 2800);
  }

  async function reload() {
    if (reloadBusy || api.saving) return;
    setReloadBusy(true);
    try {
      await api.reload();
      setDefaultError("");
      notify("模型配置已重新读取");
    } catch {
      // Context 已将中文错误写入 api.error；页面保留当前内容等待用户处理。
    } finally {
      setReloadBusy(false);
    }
  }

  async function changeDefaultModel(modelId: string) {
    if (defaultBusy || readOnly) return;
    setDefaultBusy(true);
    setDefaultError("");
    try {
      await api.setDefaultSelection({ modelId: modelId || null, levelId: null });
      notify("默认模型已更新");
    } catch (caught) {
      setDefaultError(caught instanceof Error ? caught.message : "默认模型保存失败。");
    } finally {
      setDefaultBusy(false);
    }
  }

  async function changeDefaultLevel(levelId: string) {
    if (defaultBusy || readOnly || !defaultModel) return;
    setDefaultBusy(true);
    setDefaultError("");
    try {
      await api.setDefaultSelection({ modelId: defaultModel.id, levelId: levelId || null });
      notify("默认推理档位已更新");
    } catch (caught) {
      setDefaultError(caught instanceof Error ? caught.message : "默认推理档位保存失败。");
    } finally {
      setDefaultBusy(false);
    }
  }

  async function removeProvider() {
    const gateway = editor?.initial;
    if (!gateway) return;
    await api.removeProvider(gateway.id);
    setEditor(null);
    notify("供应商已删除");
  }

  const defaultModelOptions = [["", "主配置模型"], ...api.models.map(model => [model.id, `${model.displayName} · ${api.gateways.find(gateway => gateway.id === model.gatewayId)?.name ?? ""}`] as const)] as const;
  const defaultLevelOptions = [["", "模型默认"], ...(defaultModel?.levels.map(level => [level.id, level.label] as const) ?? [])] as const;

  return (
    <div className="mp-gateways">
      <header className="mp-page-head">
        <h2>接入与模型</h2>
        <label className="mp-search"><Search aria-hidden="true" /><input aria-label="搜索供应商或模型" placeholder="搜索供应商或模型…" value={search} disabled={api.loading} onChange={event => setSearch(event.target.value)} /></label>
      </header>
      {!api.canWrite && <div className="mp-banner" role="status"><span>当前账号只有只读权限，模型配置不可修改。</span></div>}
      {api.error && <div className="mp-banner" role="alert"><span>{api.error}</span><Button className="mp-text" variant="ghost" size="sm" type="button" disabled={reloadBusy || api.saving} onClick={() => void reload()}>{reloadBusy ? "正在读取…" : "重新读取"}</Button></div>}
      {api.loading && !api.gateways.length ? <div className="mp-list-empty" aria-busy="true">正在读取模型配置…</div> : (
        <div className="mp-provider-list">
          {matches.map(gateway => {
            const models = api.models.filter(model => model.gatewayId === gateway.id);
            const currentDefault = api.selection.modelId !== null && models.some(model => model.id === api.selection.modelId);
            return (
              <button type="button" className="mp-provider-row" key={gateway.id} disabled={api.loading || api.saving} onClick={() => setEditor({ initial: gateway })}>
                <ProviderIcon url={gateway.baseUrl} />
                <span className="mp-provider-name"><strong>{gateway.name}</strong><small>{hostOf(gateway.baseUrl)} · {models.length} 个模型</small></span>
                {currentDefault && <span className="mp-default-label">默认</span>}
                <span className={`mp-key-state${gateway.keySet ? " is-set" : ""}`} aria-label={gateway.keySet ? "密钥已设置" : "未设置密钥"} title={gateway.keySet ? "密钥已设置" : "未设置密钥"}><span />{gateway.keySet ? "密钥已设置" : "未设置密钥"}</span>
                <ChevronRight aria-hidden="true" />
              </button>
            );
          })}
          {!matches.length && <div className="mp-list-empty">{api.gateways.length ? "没有匹配的供应商或模型" : "还没有供应商"}</div>}
        </div>
      )}
      <button type="button" className="mp-add-provider" disabled={readOnly} onClick={() => setAddOpen(true)}><Plus aria-hidden="true" />添加供应商</button>
      <section className="mp-default-preference" aria-label="新对话默认模型">
        <div><h3>新对话默认</h3><p>已有对话保留各自的选择。</p></div>
        <div className="mp-default-controls">
          <Select label="默认模型" value={api.selection.modelId ?? ""} options={defaultModelOptions} disabled={readOnly || defaultBusy} onChange={value => void changeDefaultModel(value)} />
          <Select label="默认推理档位" value={api.selection.levelId ?? ""} disabled={readOnly || defaultBusy || !defaultModel} options={defaultLevelOptions} onChange={value => void changeDefaultLevel(value)} />
        </div>
      </section>
      {defaultError && <p className="mp-default-error" role="alert">{defaultError}</p>}
      {addOpen && <AddProvider onClose={() => setAddOpen(false)} onChoose={preset => { setAddOpen(false); setEditor({ initial: null, preset }); }} />}
      {editor && <ProviderEditor api={api} initial={editor.initial} preset={editor.preset} onClose={() => setEditor(null)} onSaved={() => { setEditor(null); notify("供应商已保存"); }} onReloaded={() => { setEditor(null); notify("配置已重新读取，请检查最新设置"); }} onRemove={removeProvider} />}
      {toast && <div className="mp-toast" role="status"><Check aria-hidden="true" />{toast}</div>}
    </div>
  );
}

export function ModelGatewaySettings(_props: ModelGatewaySettingsProps) {
  const api = useModelGateways();
  if (!api) return null;
  return <GatewaySettingsContent api={api} />;
}
