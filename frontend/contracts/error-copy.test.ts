import { expect, test } from "vitest";
import { RequestError } from "../src/api";
import { chatReason, runReason } from "../src/lib/copy";

for (const [code, text] of [
  ["provider_protocol", "模型响应无效"],
  ["provider_incomplete_stream", "模型响应未完成"],
  ["provider_tool_arguments", "工具参数无效"],
  ["tool_limit", "工具调用次数达到本轮上限"],
  ["step_limit", "模型步骤达到本轮上限"],
] as const) {
  test(`${code} 在聊天终态、请求错误和运行结果中使用相同原因`, () => {
    expect(chatReason({ code })).toBe(text);
    expect(chatReason(new RequestError(502, code))).toBe(text);
    expect(runReason(code)).toBe(text);
  });
}

test("真实读取故障与未知代码保持原有提示", () => {
  expect(chatReason(new Error("network failure"))).toBe("读取失败，请刷新");
  expect(chatReason({ code: "unrecognized_failure" })).toBe("读取失败，请刷新");
  expect(chatReason({ code: "storage_unavailable" })).toBe("暂时无法读取记录，请刷新");
  expect(chatReason({ code: "provider_http_error" })).toBe("模型请求失败，可查看详情");
});
