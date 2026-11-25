import { object, RequestError } from "../api";

export type GatewayProtocol = "chat_completions" | "responses";
export type GatewayThinking = "omit" | "enabled" | "disabled";
export interface Gateway { id: string; name: string; baseUrl: string; protocol: GatewayProtocol; keySet: boolean; apiKeyEnv?: string | null }
export interface GatewayModelLevel { id: string; label: string; effort: string; thinking: GatewayThinking }
export interface GatewayModel { id: string; gatewayId: string; modelId: string; displayName: string; levels: GatewayModelLevel[]; vision?: boolean; maxOutputTokens?: number | null }
export interface GatewaySelection { modelId: string | null; levelId: string | null }
export interface WireSelection { gateway_id: string; model_id: string; reasoning_level: string | null }
export interface GatewayDocument { revision: number; gateways: Gateway[]; models: GatewayModel[]; selection: GatewaySelection }
export interface SessionModelSelection { session_id: string; revision: number; model_selection: WireSelection | null; configuration_revision: number | null; model: string | null; reasoning_effort: string | null; thinking: string | null }
export const modelKey = (gatewayId: string, modelId: string) => `${gatewayId}/${modelId}`;
export const EMPTY_SELECTION: GatewaySelection = { modelId: null, levelId: null };
const string = (value: unknown): string => { if (typeof value !== "string") throw new Error("模型配置响应无效"); return value; };
const nullableString = (value: unknown): string | null => value === null || value === undefined ? null : string(value);
const revision = (value: unknown): number => { if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new Error("模型配置版本无效"); return value; };
export function parseWireSelection(value: unknown): WireSelection | null {
  if (value === null || value === undefined) return null;
  const v = object(value);
  return { gateway_id: string(v.gateway_id), model_id: string(v.model_id), reasoning_level: nullableString(v.reasoning_level) };
}
export function toWireSelection(selection: GatewaySelection, models: readonly GatewayModel[]): WireSelection | null {
  if (!selection.modelId) return null;
  const model = models.find(m => m.id === selection.modelId);
  if (!model) throw new Error("所选模型已移除，请重新选择。");
  if (selection.levelId && !model.levels.some(l => l.id === selection.levelId)) throw new Error("所选推理档位已移除，请重新选择。");
  return { gateway_id: model.gatewayId, model_id: model.modelId, reasoning_level: selection.levelId };
}
export function parseGatewayDocument(raw: unknown): GatewayDocument {
  const v = object(raw);
  if (!Array.isArray(v.gateways)) throw new Error("模型配置响应无效");
  const models: GatewayModel[] = [];
  const gateways = v.gateways.map(rawGateway => {
    const g = object(rawGateway);
    if (!["chat_completions", "responses"].includes(String(g.wire_api)) || typeof g.api_key_set !== "boolean" || !Array.isArray(g.models)) throw new Error("供应商响应无效");
    const id = string(g.gateway_id);
    for (const rawModel of g.models) {
      const m = object(rawModel), wireId = string(m.id);
      const levels = Object.entries(object(m.reasoning_levels ?? {})).map(([levelId, rawLevel]) => {
        const l = object(rawLevel);
        if (l.thinking !== null && l.thinking !== undefined && l.thinking !== "enabled" && l.thinking !== "disabled") throw new Error("推理配置响应无效");
        return { id: levelId, label: nullableString(l.display_name) || levelId, effort: nullableString(l.effort) || "", thinking: l.thinking == null ? "omit" as const : l.thinking };
      });
      if (typeof m.allow_images !== "boolean" || m.max_output_tokens != null && (typeof m.max_output_tokens !== "number" || !Number.isSafeInteger(m.max_output_tokens) || m.max_output_tokens < 1)) throw new Error("模型能力响应无效");
      models.push({ id: modelKey(id, wireId), gatewayId: id, modelId: wireId, displayName: string(m.display_name), levels, vision: m.allow_images, maxOutputTokens: m.max_output_tokens ?? null } as GatewayModel);
    }
    return { id, name: string(g.name), baseUrl: string(g.base_url), protocol: g.wire_api as GatewayProtocol, keySet: g.api_key_set, apiKeyEnv: nullableString(g.api_key_env) };
  });
  const wire = parseWireSelection(v.default_selection);
  const selection = wire ? { modelId: modelKey(wire.gateway_id, wire.model_id), levelId: wire.reasoning_level } : { ...EMPTY_SELECTION };
  toWireSelection(selection, models);
  return { revision: revision(v.revision), gateways, models, selection };
}
export function parseSessionModelSelection(raw: unknown): SessionModelSelection {
  const v = object(raw);
  return { session_id: string(v.session_id), revision: revision(v.revision), model_selection: parseWireSelection(v.model_selection), configuration_revision: v.configuration_revision == null ? null : revision(v.configuration_revision), model: nullableString(v.model), reasoning_effort: nullableString(v.reasoning_effort), thinking: nullableString(v.thinking) };
}
export function gatewayUpdate(document: GatewayDocument, requestKey: string, credential?: { id: string; value?: string | null }) {
  return {
    expected_revision: document.revision, request_key: requestKey,
    gateways: document.gateways.map(g => ({
      gateway_id: g.id, name: g.name, base_url: g.baseUrl, wire_api: g.protocol,
      api_key_env: credential?.id === g.id && credential.value !== undefined ? null : g.apiKeyEnv ?? null,
      ...(credential?.id === g.id && typeof credential.value === "string" && credential.value ? { api_key: credential.value } : {}),
      ...(credential?.id === g.id && credential.value === null ? { clear_api_key: true } : {}),
      models: document.models.filter(m => m.gatewayId === g.id).map(m => ({ id: m.modelId, display_name: m.displayName, allow_images: m.vision ?? false, max_output_tokens: m.maxOutputTokens ?? null, reasoning_levels: Object.fromEntries(m.levels.map(l => [l.id, { display_name: l.label, effort: l.effort || null, thinking: l.thinking === "omit" ? null : l.thinking }])) })),
    })),
    default_selection: toWireSelection(document.selection, document.models),
  };
}
export function gatewayError(error: unknown): string {
  if (!(error instanceof RequestError)) return error instanceof Error && !/fetch|network|load|json/i.test(error.message) ? error.message : "无法确认保存结果，请重新读取配置后核对。";
  const copy: Record<string, string> = {
    revision_conflict: "配置已被其他操作更新，请重新读取后再保存。", session_busy: "会话仍在运行或排队，完成后才能切换模型。", session_closed: "会话已关闭，无法切换模型。", permission_denied: "当前账号没有修改权限。", unauthorized: "登录已失效，请重新登录。", provider_config: "模型接入配置不可用。请检查 API 地址、协议、密钥及服务端密钥保险库。", provider_not_configured: "尚未配置可用模型，请先添加供应商与模型。", target_binding_changed: "会话运行配置已变化，请重新读取会话。", invalid_input: "配置无效，请检查地址、模型 ID 和推理参数。", key_conflict: "请求标识冲突，请重新读取配置。", facts_corrupt: "保存的模型配置无法读取，请检查服务端数据。",
  };
  return copy[error.code] ?? (error.status >= 500 ? "服务暂不可用，无法确认保存结果，请重新读取后核对。" : "操作未完成，请重新读取配置后重试。");
}
