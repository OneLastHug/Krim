import { expect, test } from "vitest";
import { appendReasoningPreset, OUTPUT_TOKEN_PRESETS, REASONING_EFFORTS, REASONING_PRESETS, reasoningWirePreview } from "../src/lib/model-gateway-presets";
import { gatewayDiscoveryError, type GatewayModelLevel } from "../src/lib/model-gateways";

test("discovery preserves local configuration errors without echoing arbitrary failures", () => {
  for (const message of ["请等待配置读取完成。", "上次保存结果尚未确认，请重新读取配置后核对。", "账号已切换，请重新获取模型。", "当前账号没有获取端点模型的权限。"]) expect(gatewayDiscoveryError(new Error(message))).toBe(message);
  expect(gatewayDiscoveryError(new TypeError("Failed to fetch"))).toBe("无法获取模型，请检查网络后重试。");
  expect(gatewayDiscoveryError(new Error("untrusted upstream message"))).toBe("无法获取模型，请检查网络后重试。");
});

const preset = (id: string) => {
  const found = REASONING_PRESETS.find(item => item.id === id);
  expect(found, id).toBeTruthy();
  return found!;
};

test("reasoning presets cover common, complete, thinking and thinking-effort tiers", () => {
  expect(REASONING_PRESETS.map(item => item.id)).toEqual(["common", "complete", "thinking", "thinking-effort"]);
  const common = preset("common");
  expect(common.levels.map(level => level.effort)).toEqual(["low", "medium", "high", "xhigh"]);
  expect(common.levels.every(level => level.thinking === "omit")).toBe(true);
  expect(preset("complete").chatOnly).toBeFalsy();
  expect(preset("common").chatOnly).toBeFalsy();
  const thinking = preset("thinking");
  expect(thinking.chatOnly).toBe(true);
  expect(thinking.levels).toEqual([
    { id: "thinking-on", label: "开启思考", effort: "", thinking: "enabled" },
    { id: "thinking-off", label: "关闭思考", effort: "", thinking: "disabled" },
  ]);
});

test("the complete preset offers all seven reasoning efforts with their labels", () => {
  const complete = preset("complete");
  expect(complete.levels.map(level => level.effort)).toEqual(REASONING_EFFORTS.map(([id]) => id));
  expect(complete.levels.map(level => level.label)).toEqual(REASONING_EFFORTS.map(([, label]) => label));
  expect(complete.levels).toHaveLength(7);
  expect(complete.levels.every(level => level.thinking === "omit")).toBe(true);
});

test("the thinking-effort preset pairs an off switch with four enabled efforts", () => {
  const levels = preset("thinking-effort").levels;
  expect(levels.map(level => [level.id, level.effort, level.thinking])).toEqual([
    ["thinking-off", "", "disabled"],
    ["thinking-low", "low", "enabled"],
    ["thinking-medium", "medium", "enabled"],
    ["thinking-high", "high", "enabled"],
    ["thinking-xhigh", "xhigh", "enabled"],
  ]);
});

test("output token presets list the full documented range in ascending order", () => {
  expect([...OUTPUT_TOKEN_PRESETS]).toEqual([8000, 16000, 32000, 64000, 128000, 272000, 400000, 512000, 1000000]);
});

test("appending a preset keeps edited and custom levels untouched and never duplicates", () => {
  const edited: GatewayModelLevel = { id: "low", label: "改过的低", effort: "vendor.low", thinking: "disabled" };
  const custom: GatewayModelLevel = { id: "vendor-only", label: "自定义档位", effort: "vendor.custom", thinking: "omit" };
  const merged = appendReasoningPreset([edited, custom], preset("common"));
  expect(merged.slice(0, 2)).toEqual([edited, custom]);
  expect(merged.slice(2).map(level => level.id)).toEqual(["medium", "high", "xhigh"]);
  expect(appendReasoningPreset(merged, preset("common")).map(level => level.id)).toEqual(merged.map(level => level.id));
});

test("wire preview maps effort and thinking per protocol or explains that nothing is sent", () => {
  const level = (patch: Partial<GatewayModelLevel>): GatewayModelLevel => ({ id: "level", label: "档位", effort: "", thinking: "omit", ...patch });
  expect(reasoningWirePreview(level({ effort: "high" }), "chat_completions")).toBe('{"reasoning_effort":"high"}');
  expect(reasoningWirePreview(level({ effort: "high" }), "responses")).toBe('{"reasoning":{"effort":"high"}}');
  expect(reasoningWirePreview(level({ thinking: "enabled" }), "chat_completions")).toBe('{"thinking":{"type":"enabled"}}');
  expect(reasoningWirePreview(level({ effort: "low", thinking: "disabled" }), "chat_completions")).toBe('{"reasoning_effort":"low","thinking":{"type":"disabled"}}');
  expect(reasoningWirePreview(level({ effort: "  medium  " }), "responses")).toBe('{"reasoning":{"effort":"medium"}}');
  expect(reasoningWirePreview(level(), "chat_completions")).toBe("不发送额外推理参数");
  expect(reasoningWirePreview(level(), "responses")).toBe("不发送额外推理参数");
});
