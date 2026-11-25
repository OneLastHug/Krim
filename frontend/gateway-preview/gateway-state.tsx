// 网关与模型的本地设置状态：设置页与工作台共享同一个 Provider 实例。
// API Key 只记录"是否已设置"（keySet 布尔值），明文不进入任何状态或本地存储。
import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from "react";

export type GatewayProtocol = "chat_completions" | "responses";
export type GatewayThinking = "omit" | "enabled" | "disabled";

export interface Gateway {
  id: string;
  name: string;
  baseUrl: string;
  protocol: GatewayProtocol;
  /** 只表示是否已设置密钥，不保存密钥内容。 */
  keySet: boolean;
}

export interface GatewayModelLevel {
  id: string;
  label: string;
  effort: string;
  thinking: GatewayThinking;
}

export interface GatewayModel {
  id: string;
  gatewayId: string;
  /** 发给网关的模型标识（wire id）。 */
  modelId: string;
  displayName: string;
  levels: GatewayModelLevel[];
  /** 可选：是否支持视觉输入。 */
  vision?: boolean;
  /** 可选：最大输出 tokens；null 表示未设置。 */
  maxOutputTokens?: number | null;
}

export interface GatewaySelection {
  /** 指向 GatewayModel.id（内部标识，不是 wire modelId）。 */
  modelId: string | null;
  levelId: string | null;
}

export interface GatewayActionResult {
  ok: boolean;
  error?: string;
}

export interface GatewayPreviewApi {
  gateways: Gateway[];
  models: GatewayModel[];
  /** 当前默认选择：会话没有单独选择时生效。 */
  selection: GatewaySelection;
  /** 同 selection，按设置页语义命名的别名。 */
  defaultSelection: GatewaySelection;
  statusMessage: string;
  clearStatus: () => void;
  /** keyDraft：undefined 空保留，null 清除密钥状态，非空字符串视为设置了新密钥。 */
  saveGateway: (gateway: Gateway, keyDraft?: string | null) => GatewayActionResult;
  deleteGateway: (id: string) => GatewayActionResult;
  saveModel: (model: GatewayModel) => GatewayActionResult;
  deleteModel: (id: string) => GatewayActionResult;
  selectModel: (modelId: string | null, sessionId?: string) => void;
  selectLevel: (levelId: string | null, sessionId?: string) => void;
  setDefaultSelection: (selection: GatewaySelection) => void;
  forSession: (sessionId: string) => GatewaySelection;
}

interface GatewayStore {
  gateways: Gateway[];
  models: GatewayModel[];
  defaultSelection: GatewaySelection;
  sessionSelections: Record<string, GatewaySelection>;
}

const STORAGE_KEY = "krim.gateway-full-preview.v1";

const SEED_STORE: GatewayStore = {
  gateways: [
    { id: "gw-team", name: "团队网关", baseUrl: "https://gateway.example.com/v1", protocol: "chat_completions", keySet: true },
    { id: "gw-local", name: "本地代理", baseUrl: "http://127.0.0.1:8080/v1", protocol: "chat_completions", keySet: false },
  ],
  models: [
    {
      id: "model-general",
      gatewayId: "gw-team",
      modelId: "model-general",
      displayName: "通用模型",
      levels: [{ id: "standard", label: "标准", effort: "", thinking: "omit" }],
      vision: true,
      maxOutputTokens: null,
    },
    {
      id: "model-reasoning",
      gatewayId: "gw-team",
      modelId: "model-reasoning",
      displayName: "推理模型",
      levels: [
        { id: "low", label: "低", effort: "low", thinking: "enabled" },
        { id: "medium", label: "中", effort: "medium", thinking: "enabled" },
        { id: "high", label: "高", effort: "high", thinking: "enabled" },
        { id: "xhigh", label: "超高", effort: "xhigh", thinking: "enabled" },
      ],
      vision: false,
      maxOutputTokens: null,
    },
    {
      id: "model-local",
      gatewayId: "gw-local",
      modelId: "model-local",
      displayName: "本地模型",
      levels: [{ id: "standard", label: "标准", effort: "", thinking: "disabled" }],
      vision: false,
      maxOutputTokens: null,
    },
  ],
  defaultSelection: { modelId: "model-general", levelId: "standard" },
  sessionSelections: {},
};

function isGateway(value: unknown): value is Gateway {
  if (!value || typeof value !== "object") return false;
  const item = value as Partial<Gateway>;
  return typeof item.id === "string" && item.id !== ""
    && typeof item.name === "string"
    && typeof item.baseUrl === "string"
    && (item.protocol === "chat_completions" || item.protocol === "responses")
    && typeof item.keySet === "boolean";
}

function isModel(value: unknown): value is GatewayModel {
  if (!value || typeof value !== "object") return false;
  const item = value as Partial<GatewayModel>;
  if (typeof item.id !== "string" || item.id === "" || typeof item.gatewayId !== "string"
    || typeof item.modelId !== "string" || typeof item.displayName !== "string") return false;
  if (!Array.isArray(item.levels)) return false;
  return item.levels.every(level => {
    if (!level || typeof level !== "object") return false;
    const item2 = level as Partial<GatewayModelLevel>;
    return typeof item2.id === "string" && typeof item2.label === "string" && typeof item2.effort === "string"
      && (item2.thinking === "omit" || item2.thinking === "enabled" || item2.thinking === "disabled");
  });
}

function asSelection(value: unknown): GatewaySelection | null {
  if (!value || typeof value !== "object") return null;
  const item = value as { modelId?: unknown; levelId?: unknown };
  if (item.modelId !== null && typeof item.modelId !== "string") return null;
  if (item.levelId !== null && typeof item.levelId !== "string") return null;
  return { modelId: item.modelId ?? null, levelId: item.levelId ?? null };
}

function asSelectionMap(value: unknown): Record<string, GatewaySelection> {
  const result: Record<string, GatewaySelection> = {};
  if (!value || typeof value !== "object") return result;
  for (const [sessionId, selection] of Object.entries(value as Record<string, unknown>)) {
    const parsed = asSelection(selection);
    if (parsed) result[sessionId] = parsed;
  }
  return result;
}

function loadStore(): { store: GatewayStore; error: string } {
  if (typeof window === "undefined" || !("localStorage" in window)) {
    return { store: SEED_STORE, error: "当前环境不支持本地存储，设置只在本次运行内生效。" };
  }
  let raw: string | null;
  try {
    raw = window.localStorage.getItem(STORAGE_KEY);
  } catch {
    return { store: SEED_STORE, error: "无法读取本地保存的设置，已显示默认内容。" };
  }
  if (!raw) return { store: SEED_STORE, error: "" };
  try {
    const parsed = JSON.parse(raw) as unknown;
    const source = (parsed ?? {}) as Partial<GatewayStore>;
    return {
      store: {
        gateways: Array.isArray(source.gateways) ? source.gateways.filter(isGateway) : SEED_STORE.gateways,
        models: Array.isArray(source.models) ? source.models.filter(isModel) : SEED_STORE.models,
        defaultSelection: asSelection(source.defaultSelection) ?? SEED_STORE.defaultSelection,
        sessionSelections: asSelectionMap(source.sessionSelections),
      },
      error: "",
    };
  } catch {
    return { store: SEED_STORE, error: "本地保存的设置已损坏，已恢复默认内容。" };
  }
}

const GatewayPreviewContext = createContext<GatewayPreviewApi | null>(null);

export function GatewayPreviewProvider({ children }: { children: ReactNode }) {
  const initial = useMemo(loadStore, []);
  const [store, setStore] = useState<GatewayStore>(initial.store);
  const [statusMessage, setStatusMessage] = useState(initial.error);
  const storeRef = useRef(store);

  const apply = useCallback((next: GatewayStore) => {
    storeRef.current = next;
    setStore(next);
    if (typeof window !== "undefined" && "localStorage" in window) {
      try {
        window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      } catch {
        setStatusMessage("设置未能写入本地存储，请检查浏览器是否允许此站点存储数据。");
      }
    }
  }, []);

  const clearStatus = useCallback(() => setStatusMessage(""), []);

  const saveGateway = useCallback((gateway: Gateway, keyDraft?: string | null): GatewayActionResult => {
    const name = gateway.name.trim();
    const baseUrl = gateway.baseUrl.trim();
    if (!name || !baseUrl) return { ok: false, error: "网关名称和 API 地址不能为空。" };
    const keySet = keyDraft === null ? false : keyDraft && keyDraft.length > 0 ? true : gateway.keySet;
    const record: Gateway = { ...gateway, name, baseUrl, keySet };
    const current = storeRef.current;
    const exists = current.gateways.some(item => item.id === record.id);
    apply({
      ...current,
      gateways: exists
        ? current.gateways.map(item => item.id === record.id ? record : item)
        : [...current.gateways, record],
    });
    return { ok: true };
  }, [apply]);

  const deleteGateway = useCallback((id: string): GatewayActionResult => {
    const current = storeRef.current;
    const target = current.gateways.find(item => item.id === id);
    if (!target) return { ok: false, error: "要删除的网关不存在。" };
    const referencing = current.models.filter(item => item.gatewayId === id);
    if (referencing.length > 0) {
      return { ok: false, error: `网关「${target.name}」被 ${referencing.length} 个模型使用，先删除或转移这些模型，再删除网关。` };
    }
    apply({ ...current, gateways: current.gateways.filter(item => item.id !== id) });
    return { ok: true };
  }, [apply]);

  const saveModel = useCallback((model: GatewayModel): GatewayActionResult => {
    const displayName = model.displayName.trim();
    const wireModelId = model.modelId.trim();
    if (!displayName || !wireModelId) return { ok: false, error: "模型显示名称和模型 ID 不能为空。" };
    const current = storeRef.current;
    if (!current.gateways.some(item => item.id === model.gatewayId)) {
      return { ok: false, error: "模型所属网关不存在，请重新选择。" };
    }
    const clash = current.models.find(item =>
      item.id !== model.id && item.gatewayId === model.gatewayId && item.modelId === wireModelId);
    if (clash) return { ok: false, error: `此网关下已有模型 ID「${wireModelId}」（${clash.displayName}），请换一个。` };
    const record: GatewayModel = {
      ...model,
      displayName,
      modelId: wireModelId,
      levels: model.levels.map(level => ({ ...level, id: level.id.trim(), label: level.label.trim(), effort: level.effort.trim() })),
    };
    const exists = current.models.some(item => item.id === record.id);
    apply({
      ...current,
      models: exists
        ? current.models.map(item => item.id === record.id ? record : item)
        : [...current.models, record],
    });
    return { ok: true };
  }, [apply]);

  const deleteModel = useCallback((id: string): GatewayActionResult => {
    const current = storeRef.current;
    if (!current.models.some(item => item.id === id)) return { ok: false, error: "要删除的模型不存在。" };
    const sessionSelections: Record<string, GatewaySelection> = {};
    for (const [sessionId, selection] of Object.entries(current.sessionSelections)) {
      if (selection.modelId !== id) sessionSelections[sessionId] = selection;
    }
    apply({
      ...current,
      models: current.models.filter(item => item.id !== id),
      defaultSelection: current.defaultSelection.modelId === id ? { modelId: null, levelId: null } : current.defaultSelection,
      sessionSelections,
    });
    return { ok: true };
  }, [apply]);

  const selectModel = useCallback((modelId: string | null, sessionId?: string) => {
    const current = storeRef.current;
    const model = modelId ? current.models.find(item => item.id === modelId) ?? null : null;
    const next: GatewaySelection = { modelId: model?.id ?? null, levelId: model?.levels[0]?.id ?? null };
    if (sessionId) {
      apply({ ...current, sessionSelections: { ...current.sessionSelections, [sessionId]: next } });
    } else {
      apply({ ...current, defaultSelection: next });
    }
  }, [apply]);

  const selectLevel = useCallback((levelId: string | null, sessionId?: string) => {
    const current = storeRef.current;
    const base = sessionId ? current.sessionSelections[sessionId] ?? current.defaultSelection : current.defaultSelection;
    const next: GatewaySelection = { ...base, levelId };
    if (sessionId) {
      apply({ ...current, sessionSelections: { ...current.sessionSelections, [sessionId]: next } });
    } else {
      apply({ ...current, defaultSelection: next });
    }
  }, [apply]);

  const setDefaultSelection = useCallback((selection: GatewaySelection) => {
    const current = storeRef.current;
    apply({ ...current, defaultSelection: { modelId: selection.modelId ?? null, levelId: selection.levelId ?? null } });
  }, [apply]);

  const forSession = useCallback((sessionId: string): GatewaySelection =>
    storeRef.current.sessionSelections[sessionId] ?? storeRef.current.defaultSelection, []);

  const value = useMemo<GatewayPreviewApi>(() => ({
    gateways: store.gateways,
    models: store.models,
    selection: store.defaultSelection,
    defaultSelection: store.defaultSelection,
    statusMessage,
    clearStatus,
    saveGateway,
    deleteGateway,
    saveModel,
    deleteModel,
    selectModel,
    selectLevel,
    setDefaultSelection,
    forSession,
  }), [store, statusMessage, clearStatus, saveGateway, deleteGateway, saveModel, deleteModel, selectModel, selectLevel, setDefaultSelection, forSession]);

  return <GatewayPreviewContext.Provider value={value}>{children}</GatewayPreviewContext.Provider>;
}

export function useGatewayPreview(): GatewayPreviewApi {
  const value = useContext(GatewayPreviewContext);
  if (!value) throw new Error("useGatewayPreview 必须在 GatewayPreviewProvider 内使用。");
  return value;
}

/** 已有上层 Provider 时返回它，供组件避免重复包裹导致状态分叉。 */
export function useOptionalGatewayPreview(): GatewayPreviewApi | null {
  return useContext(GatewayPreviewContext);
}
