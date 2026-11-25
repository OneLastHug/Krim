import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { request, RequestError } from "../api";
import type { Actor } from "../lib/session-contracts";
import { EMPTY_SELECTION, gatewayError, gatewayUpdate, modelKey, parseGatewayDocument, toWireSelection, type Gateway, type GatewayDocument, type GatewayModel, type GatewaySelection } from "../lib/model-gateways";

export interface ModelGatewayContext extends GatewayDocument {
  loading: boolean;
  error: string;
  saving: boolean;
  canWrite: boolean;
  reload: () => Promise<void>;
  saveProvider: (gateway: Gateway, models: GatewayModel[], keyDraft?: string | null) => Promise<void>;
  removeProvider: (id: string) => Promise<void>;
  setDefaultSelection: (selection: GatewaySelection) => Promise<void>;
  wireSelection: (selection: GatewaySelection) => ReturnType<typeof toWireSelection>;
}
const Context = createContext<ModelGatewayContext | null>(null);
const EMPTY: GatewayDocument = { revision: 0, gateways: [], models: [], selection: EMPTY_SELECTION };
export function ModelGatewaysProvider({ apiKey, identity, children }: { apiKey: string; identity: Actor | null; children: ReactNode }) {
  const scope = JSON.stringify([apiKey, identity?.principal_id, identity?.generation]);
  const scopeRef = useRef(scope); scopeRef.current = scope;
  const [state, setState] = useState<{ scope: string; document: GatewayDocument; loading: boolean; error: string; saving: boolean }>({ scope, document: EMPTY, loading: true, error: "", saving: false });
  const stateRef = useRef(state); stateRef.current = state;
  const live = useRef(true), epoch = useRef(0), writing = useRef(false), unconfirmed = useRef(false), loaded = useRef("");
  useEffect(() => { live.current = true; return () => { live.current = false; }; }, []);
  const canWrite = !!identity?.workspaces.some(w => w.role !== "viewer");
  const reload = useCallback(async () => {
    if (!identity || writing.current) return;
    const ticket = ++epoch.current;
    loaded.current = "";
    const current = () => live.current && scopeRef.current === scope && ticket === epoch.current;
    setState(s => ({ scope, document: s.scope === scope ? s.document : EMPTY, loading: true, error: "", saving: false }));
    try {
      const document = parseGatewayDocument(await request("/v2/me/model-gateways", apiKey));
      if (current()) { loaded.current = scope; unconfirmed.current = false; const next = { scope, document, loading: false, error: "", saving: false }; stateRef.current = next; setState(next); }
    } catch (error) {
      if (current()) setState(s => ({ ...s, loading: false, error: gatewayError(error) }));
      throw error;
    }
  }, [apiKey, scope, !!identity]);
  useEffect(() => {
    epoch.current++; writing.current = false; unconfirmed.current = false; loaded.current = "";
    setState({ scope, document: EMPTY, loading: !!identity, error: "", saving: false });
    if (identity) void reload().catch(() => {});
    return () => { epoch.current++; };
  }, [scope, reload]);
  async function save(document: GatewayDocument, credential?: { id: string; value?: string | null }) {
    if (!canWrite) throw new Error("当前账号没有修改模型配置的权限。");
    if (writing.current || loaded.current !== scope || stateRef.current.loading) throw new Error("请等待配置读取完成。");
    if (unconfirmed.current) throw new Error("上次保存结果尚未确认，请重新读取配置后核对。");
    writing.current = true;
    const ticket = ++epoch.current;
    const current = () => live.current && scopeRef.current === scope && ticket === epoch.current;
    setState(s => ({ ...s, saving: true, error: "" }));
    try {
      const body = gatewayUpdate(document, crypto.randomUUID(), credential);
      const saved = parseGatewayDocument(await request("/v2/me/model-gateways", apiKey, body, undefined, "PUT"));
      if (!current()) throw new Error("账号已切换，请重新读取模型配置。");
      const next = { scope, document: saved, loading: false, error: "", saving: false };
      stateRef.current = next; setState(next);
      window.dispatchEvent(new Event("krim:model-gateways-updated"));
    } catch (error) {
      if (current()) {
        unconfirmed.current = !(error instanceof RequestError) || error.status >= 500 || error.code === "revision_conflict";
        setState(s => ({ ...s, error: gatewayError(error), saving: false }));
      }
      throw new Error(gatewayError(error));
    } finally { if (scopeRef.current === scope) writing.current = false; }
  }
  const view = state.scope === scope ? state : { scope, document: EMPTY, loading: !!identity, error: "", saving: false };
  const api: ModelGatewayContext = {
    ...view.document, loading: view.loading, error: view.error, saving: view.saving, canWrite,
    reload,
    saveProvider: async (gateway, models, keyDraft) => {
      const document = stateRef.current.document;
      const previous = document.gateways.find(g => g.id === gateway.id);
      const gateways = previous ? document.gateways.map(g => g.id === gateway.id ? { ...previous, ...gateway } : g) : [...document.gateways, gateway];
      const normalized = models.map(m => ({ ...m, id: modelKey(gateway.id, m.modelId), gatewayId: gateway.id }));
      const allModels = [...document.models.filter(m => m.gatewayId !== gateway.id), ...normalized];
      let selection = document.selection;
      const selected = document.models.find(m => m.id === selection.modelId);
      if (selected?.gatewayId === gateway.id) {
        const index = models.findIndex(m => m.id === selected.id);
        const replacement = normalized[index];
        selection = replacement ? { modelId: replacement.id, levelId: replacement.levels.some(l => l.id === selection.levelId) ? selection.levelId : null } : { ...EMPTY_SELECTION };
      }
      await save({ ...document, gateways, models: allModels, selection }, { id: gateway.id, value: keyDraft });
    },
    removeProvider: async id => {
      const document = stateRef.current.document;
      const model = document.models.find(m => m.id === document.selection.modelId);
      await save({ ...document, gateways: document.gateways.filter(g => g.id !== id), models: document.models.filter(m => m.gatewayId !== id), selection: model?.gatewayId === id ? { ...EMPTY_SELECTION } : document.selection });
    },
    setDefaultSelection: async selection => { await save({ ...stateRef.current.document, selection }); },
    wireSelection: selection => toWireSelection(selection, view.document.models),
  };
  return <Context.Provider value={api}>{children}</Context.Provider>;
}
export function useModelGateways() { return useContext(Context); }
