import { object } from "../api";

export type InteractionMode = "execute" | "plan";
export type InteractionRequestKind = "user_input" | "plan_confirmation";
export type InteractionRequestStatus = "pending" | "answered" | "cancelled" | "superseded";
export type InteractionQuestionMode = "single" | "multiple" | "text";

export interface InteractionOption {
  id: string;
  label: string;
  description: string;
}

export interface InteractionQuestion {
  id: string;
  prompt: string;
  mode: InteractionQuestionMode;
  options: InteractionOption[];
  allow_text: boolean;
}

export interface UserInputPayload {
  questions: InteractionQuestion[];
}

export interface PlanConfirmationPayload {
  plan: string;
}

interface InteractionRequestBase<TKind extends InteractionRequestKind, TPayload> {
  id: string;
  kind: TKind;
  status: InteractionRequestStatus;
  revision: number;
  workspace_id: string;
  owner_id: string;
  session_id: string;
  run_id: string | null;
  turn_id: string;
  request_key: string;
  mode_revision: number;
  payload: TPayload;
  response: unknown;
  created_at: number;
  updated_at: number;
}

export type UserInputRequest = InteractionRequestBase<"user_input", UserInputPayload>;
export type PlanConfirmationRequest = InteractionRequestBase<"plan_confirmation", PlanConfirmationPayload>;
export type InteractionRequest = UserInputRequest | PlanConfirmationRequest;

export interface InteractionState {
  mode: InteractionMode;
  revision: number;
  pending: InteractionRequest[];
}

export interface InteractionAnswer {
  question_id: string;
  option_ids: string[];
  text: string | null;
}

export interface InteractionAnswerBody {
  request_key: string;
  expected_revision: number;
  answers: InteractionAnswer[];
  confirmed: boolean | null;
}

export interface InteractionCancelBody {
  request_key: string;
  expected_revision: number;
  reason: string;
}

function text(value: unknown): value is string {
  return typeof value === "string";
}

function revision(value: unknown, allowZero = false): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= (allowZero ? 0 : 1);
}

function parseOption(raw: unknown): InteractionOption {
  const value = object(raw);
  if (!text(value.id) || !value.id || !text(value.label) || !value.label ||
      (value.description !== undefined && !text(value.description))) {
    throw new Error("问题选项响应无效");
  }
  return { id: value.id, label: value.label, description: text(value.description) ? value.description : "" };
}

function parseQuestion(raw: unknown): InteractionQuestion {
  const value = object(raw);
  if (!text(value.id) || !value.id || !text(value.prompt) || !value.prompt ||
      !["single", "multiple", "text"].includes(String(value.mode)) ||
      !Array.isArray(value.options) || typeof value.allow_text !== "boolean") {
    throw new Error("问题响应无效");
  }
  const options = value.options.map(parseOption);
  if ((value.mode === "text" && options.length > 0) || (value.mode !== "text" && options.length === 0)) {
    throw new Error("问题选项响应无效");
  }
  return {
    id: value.id,
    prompt: value.prompt,
    mode: value.mode as InteractionQuestionMode,
    options,
    allow_text: value.allow_text,
  };
}

export function parseInteractionRequest(raw: unknown): InteractionRequest {
  const value = object(raw);
  if (!text(value.id) || !text(value.workspace_id) || !text(value.owner_id) ||
      !text(value.session_id) || (value.run_id !== null && !text(value.run_id)) ||
      !text(value.turn_id) || !text(value.request_key) ||
      !revision(value.revision) || !revision(value.mode_revision, true) ||
      !["user_input", "plan_confirmation"].includes(String(value.kind)) ||
      !["pending", "answered", "cancelled", "superseded"].includes(String(value.status)) ||
      typeof value.created_at !== "number" || !Number.isFinite(value.created_at) ||
      typeof value.updated_at !== "number" || !Number.isFinite(value.updated_at)) {
    throw new Error("交互请求响应无效");
  }

  if (value.kind === "user_input") {
    const payload = object(value.payload);
    if (!Array.isArray(payload.questions) || payload.questions.length < 1) {
      throw new Error("问题响应无效");
    }
    const questions = payload.questions.map(parseQuestion);
    if (new Set(questions.map(question => question.id)).size !== questions.length) {
      throw new Error("问题响应无效");
    }
    return { ...value, kind: "user_input", payload: { questions } } as UserInputRequest;
  }

  const payload = object(value.payload);
  if (!text(payload.plan) || !payload.plan.trim()) {
    throw new Error("计划响应无效");
  }
  return { ...value, kind: "plan_confirmation", payload: { plan: payload.plan } } as PlanConfirmationRequest;
}

export function parseInteractionState(raw: unknown): InteractionState {
  const value = object(raw);
  if (!["execute", "plan"].includes(String(value.mode)) || !revision(value.revision, true) ||
      !Array.isArray(value.pending)) {
    throw new Error("交互状态响应无效");
  }
  const pending = value.pending.map(parseInteractionRequest);
  if (pending.some(item => item.status !== "pending")) {
    throw new Error("交互状态响应无效");
  }
  return { mode: value.mode as InteractionMode, revision: value.revision, pending };
}
