import type { GatewayModelLevel, GatewayProtocol } from "./model-gateways";

// Reasoning values follow Magpie provider/modelprefs.go; output picks follow the user's preferred range.
export const REASONING_EFFORTS = [
  ["none", "无"], ["minimal", "极低"], ["low", "低"], ["medium", "中"],
  ["high", "高"], ["xhigh", "超高"], ["max", "最高"],
] as const;
export const OUTPUT_TOKEN_PRESETS = [8000, 16000, 32000, 64000, 128000, 272000, 400000, 512000, 1000000] as const;
export const PROTOCOL_HINTS: Record<GatewayProtocol, string> = {
  chat_completions: "填写 API 基础地址（通常以 /v1 结尾）；请求发送到 /chat/completions。",
  responses: "填写 API 基础地址（通常以 /v1 结尾）；请求发送到 /responses，推理强度使用 reasoning.effort。",
};
export interface ReasoningPreset { id: string; name: string; hint: string; levels: GatewayModelLevel[]; chatOnly?: boolean }
const efforts = (ids: string[]): GatewayModelLevel[] => REASONING_EFFORTS.filter(([id]) => ids.includes(id)).map(([id, label]) => ({ id, label, effort: id, thinking: "omit" }));
export const REASONING_PRESETS: ReasoningPreset[] = [
  { id: "common", name: "常用强度", hint: "low / medium / high / xhigh；thinking 跟随模型。", levels: efforts(["low", "medium", "high", "xhigh"]) },
  { id: "complete", name: "完整强度", hint: "none / minimal / low / medium / high / xhigh / max。", levels: efforts(REASONING_EFFORTS.map(([id]) => id)) },
  { id: "thinking", name: "思考开关", hint: "仅发送 thinking.type = enabled 或 disabled，不发送 effort。", chatOnly: true, levels: [
    { id: "thinking-on", label: "开启思考", effort: "", thinking: "enabled" },
    { id: "thinking-off", label: "关闭思考", effort: "", thinking: "disabled" },
  ] },
  { id: "thinking-effort", name: "思考开关与强度", hint: "关闭思考，或开启思考并发送 low / medium / high / xhigh。", chatOnly: true, levels: [
    { id: "thinking-off", label: "关闭思考", effort: "", thinking: "disabled" },
    ...efforts(["low", "medium", "high", "xhigh"]).map(level => ({ ...level, id: `thinking-${level.id}`, thinking: "enabled" as const })),
  ] },
];

export function appendReasoningPreset(current: readonly GatewayModelLevel[], preset: ReasoningPreset): GatewayModelLevel[] {
  const ids = new Set(current.map(level => level.id));
  // A preset must never overwrite an edited label or a custom parameter mapping.
  return [...current, ...preset.levels.filter(level => !ids.has(level.id)).map(level => ({ ...level }))];
}
export function reasoningWirePreview(level: GatewayModelLevel, protocol: GatewayProtocol): string {
  const body: Record<string, unknown> = {};
  if (level.effort.trim()) {
    if (protocol === "responses") body.reasoning = { effort: level.effort.trim() };
    else body.reasoning_effort = level.effort.trim();
  }
  if (level.thinking !== "omit") body.thinking = { type: level.thinking };
  return Object.keys(body).length ? JSON.stringify(body) : "不发送额外推理参数";
}
