import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi, type Mock } from "vitest";
import { RequestError } from "../src/api";
import { UserQuestion, type UserQuestionProps } from "../src/components/UserQuestion";
import { PlanModeStatus } from "../src/components/PlanModeStatus";
import {
  parseInteractionRequest, parseInteractionState,
  type InteractionAnswerBody, type InteractionCancelBody,
  type InteractionState, type PlanConfirmationRequest, type UserInputRequest,
} from "../src/lib/interaction-contracts";

let root: Root;
let host: HTMLDivElement;
const question: UserInputRequest = {
  id: "question-request", kind: "user_input", status: "pending", revision: 1,
  workspace_id: "workspace-a", owner_id: "owner-a", session_id: "session-a", run_id: "run-a",
  turn_id: "turn-a", request_key: "ask-key", mode_revision: 0, response: null,
  created_at: 1, updated_at: 1,
  payload: { questions: [
    { id: "choice", prompt: "选择一种方案", mode: "single", allow_text: true, options: [
      { id: "first", label: "方案一", description: "简要说明" },
      { id: "second", label: "方案二", description: "另一种方案" },
    ] },
    { id: "details", prompt: "补充要求", mode: "text", allow_text: true, options: [] },
    { id: "many", prompt: "选择多个", mode: "multiple", allow_text: false, options: [
      { id: "one", label: "甲", description: "" }, { id: "two", label: "乙", description: "" },
    ] },
  ] },
};
const planRequest: PlanConfirmationRequest = {
  ...question,
  id: "plan-request", kind: "plan_confirmation", revision: 2, mode_revision: 5,
  payload: { plan: "1. 检查现有行为\n2. 更新实现" },
};
const planState: InteractionState = { mode: "plan", revision: 5, pending: [planRequest] };

beforeEach(() => {
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove(); vi.restoreAllMocks();
});
async function renderQuestion(request: UserInputRequest,
  onAnswer: UserQuestionProps["onAnswer"] = vi.fn(async () => undefined),
  onCancel: UserQuestionProps["onCancel"] = vi.fn(async () => undefined)) {
  await act(async () => root.render(<UserQuestion request={request} onAnswer={onAnswer} onCancel={onCancel} />));
  return { onAnswer, onCancel };
}
type PlanAnswerMock = Mock<(request: PlanConfirmationRequest, body: InteractionAnswerBody) => Promise<unknown>>;
type PlanCancelMock = Mock<(request: PlanConfirmationRequest, body: InteractionCancelBody) => Promise<unknown>>;
function planProps(overrides: {
  state?: InteractionState; request?: PlanConfirmationRequest | null; disabled?: boolean;
  onConfirm?: PlanAnswerMock; onReject?: PlanAnswerMock; onCancel?: PlanCancelMock;
} = {}) {
  return { state: overrides.state ?? planState, request: overrides.request ?? planRequest,
    disabled: overrides.disabled,
    onConfirm: overrides.onConfirm ?? vi.fn(async (_request: PlanConfirmationRequest, _body: InteractionAnswerBody) => undefined),
    onReject: overrides.onReject ?? vi.fn(async (_request: PlanConfirmationRequest, _body: InteractionAnswerBody) => undefined),
    onCancel: overrides.onCancel ?? vi.fn(async (_request: PlanConfirmationRequest, _body: InteractionCancelBody) => undefined) };
}
function button(label: string) {
  const found = [...host.querySelectorAll("button")].find(item => item.textContent?.trim() === label);
  expect(found).toBeTruthy(); return found!;
}
function textareas() { return [...host.querySelectorAll<HTMLTextAreaElement>("textarea")]; }
function fill(element: HTMLTextAreaElement, value: string) {
  const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!;
  setter.call(element, value); element.dispatchEvent(new Event("input", { bubbles: true }));
}
function completeAnswers() {
  host.querySelectorAll<HTMLInputElement>('input[type="radio"]')[0]!.click();
  const checks = host.querySelectorAll<HTMLInputElement>('input[type="checkbox"]');
  checks[0]!.click(); checks[1]!.click();
  fill(textareas()[1]!, "请保留兼容");
}

test("user_question_requires_complete_answers_and_submits_single_multiple_and_text_answers", async () => {
  const onAnswer = vi.fn(async (_request: UserInputRequest, _body: InteractionAnswerBody) => undefined);
  await renderQuestion(question, onAnswer);
  await act(async () => { button("提交回答").click(); });
  expect(host.textContent).toContain("请回答所有问题后再提交");
  expect(onAnswer).not.toHaveBeenCalled();
  const radios = host.querySelectorAll<HTMLInputElement>('input[type="radio"]');
  expect(radios[0]!.name).toBe(`interaction-${question.id}-choice`);
  await act(async () => { completeAnswers(); });
  await act(async () => { button("提交回答").click(); });
  expect(onAnswer).toHaveBeenCalledOnce();
  expect(onAnswer.mock.calls[0]![0]).toBe(question);
  expect(onAnswer.mock.calls[0]![1]).toMatchObject({
    expected_revision: 1, confirmed: null,
    answers: [
      { question_id: "choice", option_ids: ["first"], text: null },
      { question_id: "details", option_ids: [], text: "请保留兼容" },
      { question_id: "many", option_ids: ["one", "two"], text: null },
    ],
  });
  expect(onAnswer.mock.calls[0]![1].request_key).toBeTruthy();
});

test("question_resets_on_new_revision_and_old_callback_cannot_replace_new_state", async () => {
  let finish!: () => void;
  const oldAnswer = vi.fn((_request: UserInputRequest, _body: InteractionAnswerBody) =>
    new Promise<void>(resolve => { finish = resolve; }));
  await renderQuestion(question, oldAnswer);
  await act(async () => { completeAnswers(); });
  await act(async () => { button("提交回答").click(); });
  expect(button("提交中…").disabled).toBe(true);

  await renderQuestion({ ...question, revision: 2 });
  expect(button("提交回答").disabled).toBe(false);
  expect(host.querySelectorAll<HTMLInputElement>('input[type="radio"]')[0]!.checked).toBe(false);
  expect(textareas()[1]!.value).toBe("");
  await act(async () => { finish(); await Promise.resolve(); });
  expect(host.textContent).not.toContain("提交未完成");
  expect(button("提交回答").disabled).toBe(false);
});

test("answer_retry_reuses_request_key_until_input_changes", async () => {
  const failure = new RequestError(503, "execution_error");
  const onAnswer = vi.fn(async (_request: UserInputRequest, _body: InteractionAnswerBody) => { throw failure; });
  const onCancel = vi.fn(async (_request: UserInputRequest, _body: InteractionCancelBody) => { throw failure; });
  await renderQuestion(question, onAnswer, onCancel);
  await act(async () => { completeAnswers(); });
  await act(async () => { button("提交回答").click(); });
  await act(async () => { button("提交回答").click(); });
  expect(onAnswer).toHaveBeenCalledTimes(2);
  expect(onAnswer.mock.calls[1]![1].request_key).toBe(onAnswer.mock.calls[0]![1].request_key);
  expect(host.textContent).toContain("提交未完成，请重试");
  await act(async () => { fill(textareas()[1]!, "修改后的补充"); });
  await act(async () => { button("提交回答").click(); });
  expect(onAnswer).toHaveBeenCalledTimes(3);
  expect(onAnswer.mock.calls[2]![1].request_key).not.toBe(onAnswer.mock.calls[0]![1].request_key);

  await act(async () => { button("取消问题").click(); });
  await act(async () => { button("取消问题").click(); });
  expect(onCancel).toHaveBeenCalledTimes(2);
  expect(onCancel.mock.calls[1]![1].request_key).toBe(onCancel.mock.calls[0]![1].request_key);
});

test("failed_answer_keeps_input_and_uses_request_error_copy", async () => {
  const conflict = vi.fn(async (_request: UserInputRequest, _body: InteractionAnswerBody) => {
    throw new RequestError(409, "input_revision_conflict");
  });
  await renderQuestion(question, conflict);
  await act(async () => {
    host.querySelectorAll<HTMLInputElement>('input[type="radio"]')[0]!.click();
    const checks = host.querySelectorAll<HTMLInputElement>('input[type="checkbox"]');
    checks[0]!.click(); checks[1]!.click();
    fill(textareas()[1]!, "冲突前的输入");
  });
  await act(async () => { button("提交回答").click(); });
  expect(host.textContent).toContain("问题已更新，请刷新后重试");
  expect(textareas()[1]!.value).toBe("冲突前的输入");
  expect(button("提交回答").disabled).toBe(false);
});

test("terminal_question_is_read_only_and_cancel_sends_revisioned_body", async () => {
  const onCancel = vi.fn(async (_request: UserInputRequest, _body: InteractionCancelBody) => undefined);
  await renderQuestion(question, vi.fn(async () => undefined), onCancel);
  await act(async () => { button("取消问题").click(); });
  expect(onCancel).toHaveBeenCalledOnce();
  expect(onCancel.mock.calls[0]![1]).toMatchObject({ expected_revision: 1, reason: "user_cancelled" });
  expect(onCancel.mock.calls[0]![1].request_key).toBeTruthy();
  await renderQuestion({ ...question, status: "superseded" });
  expect(host.textContent).toContain("已由新问题替代");
  expect(host.querySelector("button")).toBeNull();
});

test("plan_mode_confirm_reject_and_cancel_are_distinct_actions_with_stable_keys", async () => {
  const props = planProps();
  const render = () => act(async () => root.render(<PlanModeStatus {...props} />));
  await render();
  expect(host.textContent).toContain("计划模式");
  expect(host.textContent).toContain(planRequest.payload.plan);
  await act(async () => { button("按计划执行").click(); });
  expect(props.onConfirm).toHaveBeenCalledOnce();
  expect(props.onConfirm.mock.calls[0]![0]).toBe(planRequest);
  expect(props.onConfirm.mock.calls[0]![1]).toMatchObject({ expected_revision: 2, answers: [], confirmed: true });
  expect(props.onConfirm.mock.calls[0]![1].request_key).toBeTruthy();
  await render();
  await act(async () => { button("继续修改计划").click(); });
  expect(props.onReject).toHaveBeenCalledOnce();
  expect(props.onReject.mock.calls[0]![1]).toMatchObject({ expected_revision: 2, answers: [], confirmed: false });
  expect(props.onReject.mock.calls[0]![1].request_key).not.toBe(props.onConfirm.mock.calls[0]![1].request_key);
  await render();
  await act(async () => { button("取消请求").click(); });
  expect(props.onCancel).toHaveBeenCalledOnce();
  expect(props.onCancel.mock.calls[0]![0]).toBe(planRequest);
  expect(props.onCancel.mock.calls[0]![1]).toMatchObject({ expected_revision: 2, reason: "user_cancelled" });
  expect(props.onCancel.mock.calls[0]![1].request_key).toBeTruthy();
});

test("plan_confirm_retry_reuses_request_key_until_action_changes", async () => {
  const failure = new RequestError(503, "execution_error");
  const props = planProps({
    onConfirm: vi.fn(async (_request: PlanConfirmationRequest, _body: InteractionAnswerBody) => { throw failure; }),
    onReject: vi.fn(async (_request: PlanConfirmationRequest, _body: InteractionAnswerBody) => { throw failure; }),
  });
  const render = () => act(async () => root.render(<PlanModeStatus {...props} />));
  await render();
  await act(async () => { button("按计划执行").click(); });
  await act(async () => { button("按计划执行").click(); });
  expect(props.onConfirm).toHaveBeenCalledTimes(2);
  expect(props.onConfirm.mock.calls[1]![1].request_key).toBe(props.onConfirm.mock.calls[0]![1].request_key);
  expect(host.textContent).toContain("操作未完成，请重试");
  await act(async () => { button("继续修改计划").click(); });
  expect(props.onReject).toHaveBeenCalledTimes(1);
  expect(props.onReject.mock.calls[0]![1].request_key).not.toBe(props.onConfirm.mock.calls[0]![1].request_key);
});

test("plan_status_disables_actions_when_disabled_or_terminal", async () => {
  const props = planProps();
  await act(async () => root.render(<PlanModeStatus {...props} disabled />));
  expect(button("按计划执行").disabled).toBe(true);
  expect(button("继续修改计划").disabled).toBe(true);
  expect(button("取消请求").disabled).toBe(true);
  expect(props.onConfirm).not.toHaveBeenCalled();
  await act(async () => root.render(<PlanModeStatus {...props} request={{ ...planRequest, status: "answered" }} />));
  expect(host.textContent).toContain("计划确认已处理");
  expect(host.querySelector("button")).toBeNull();
});

test("plan_status_locks_stale_proposal_when_mode_or_revision_no_longer_matches", async () => {
  let fail!: (error: unknown) => void;
  const props = planProps({
    onConfirm: vi.fn((_request: PlanConfirmationRequest, _body: InteractionAnswerBody) =>
      new Promise<void>((_resolve, reject) => { fail = reject; })),
  });
  await act(async () => root.render(<PlanModeStatus {...props} />));
  await act(async () => { button("按计划执行").click(); });
  expect(button("确认中…").disabled).toBe(true);
  await act(async () => root.render(<PlanModeStatus {...props} state={{ ...planState, revision: 6 }} />));
  expect(host.textContent).toContain("计划已更新，请等待新的确认请求");
  expect(button("按计划执行").disabled).toBe(true);
  expect(button("继续修改计划").disabled).toBe(true);
  expect(button("取消请求").disabled).toBe(false);
  await act(async () => { fail(new Error("stale")); await Promise.resolve(); });
  expect(host.textContent).not.toContain("操作未完成");
  await act(async () => root.render(<PlanModeStatus {...props} state={{ ...planState, mode: "execute" }} />));
  expect(host.textContent).toContain("计划已更新，请等待新的确认请求");
  expect(button("按计划执行").disabled).toBe(true);
  expect(props.onReject).not.toHaveBeenCalled();
});

test("interaction_contracts_parse_wire_state_and_reject_malformed_payloads", () => {
  const state = parseInteractionState({ mode: "plan", revision: 3, pending: [{ ...planRequest }] });
  expect(state).toEqual({ mode: "plan", revision: 3, pending: [planRequest] });
  expect(parseInteractionRequest({ ...planRequest, status: "answered", revision: 3 }).status).toBe("answered");
  expect(() => parseInteractionState({ mode: "paused", revision: 1, pending: [] })).toThrow("交互状态响应无效");
  expect(() => parseInteractionState({ mode: "plan", revision: 1, pending: [{ ...planRequest, status: "cancelled" }] }))
    .toThrow("交互状态响应无效");
  expect(() => parseInteractionRequest({ ...question, payload: { questions: [] } })).toThrow("问题响应无效");
  expect(() => parseInteractionRequest({
    ...question,
    payload: { questions: [{ id: "text", prompt: "问", mode: "text", allow_text: true, options: [{ id: "x", label: "x", description: "" }] }] },
  })).toThrow("问题选项响应无效");
  expect(() => parseInteractionRequest({ ...planRequest, payload: { plan: "   " } })).toThrow("计划响应无效");
  expect(() => parseInteractionRequest({ ...question, revision: 0 })).toThrow("交互请求响应无效");
});
