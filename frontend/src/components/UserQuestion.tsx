import { useLayoutEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import { CircleHelp } from "lucide-react";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import {
  type InteractionAnswer,
  type InteractionAnswerBody,
  type InteractionCancelBody,
  type InteractionOption,
  type InteractionQuestion,
  type UserInputRequest,
} from "../lib/interaction-contracts";

type Draft = { optionIds: string[]; text: string };
type FormState = { identity: string; answers: Record<string, Draft>; busy: boolean; error: string };
type LastAction = { identity: string; key: string; payload: string };

export interface UserQuestionProps {
  request: UserInputRequest;
  onAnswer: (request: UserInputRequest, body: InteractionAnswerBody) => Promise<unknown>;
  onCancel: (request: UserInputRequest, body: InteractionCancelBody) => Promise<unknown>;
  disabled?: boolean;
}

const terminalLabels: Record<string, string> = {
  answered: "已回答",
  cancelled: "已取消",
  superseded: "已由新问题替代",
};

function actionError(error: unknown, fallback: string): string {
  if (error instanceof RequestError) {
    if (error.code === "input_revision_conflict" || error.code === "revision_conflict") return "问题已更新，请刷新后重试";
    if (error.code === "key_conflict") return "请求状态不一致，请刷新后重试";
    if (error.code === "session_closed") return "对话已关闭";
    if (error.code === "not_found") return "问题已结束";
  }
  return fallback;
}

function emptyAnswers(questions: InteractionQuestion[]): Record<string, Draft> {
  return Object.fromEntries(questions.map(question => [question.id, { optionIds: [], text: "" }]));
}

function QuestionOption({
  question,
  requestId,
  option,
  checked,
  disabled,
  onChange,
}: {
  question: InteractionQuestion;
  requestId: string;
  option: InteractionOption;
  checked: boolean;
  disabled: boolean;
  onChange: (checked: boolean) => void;
}) {
  return <label className="question-option">
    <input
      type={question.mode === "single" ? "radio" : "checkbox"}
      name={question.mode === "single" ? `interaction-${requestId}-${question.id}` : undefined}
      value={option.id}
      checked={checked}
      disabled={disabled}
      onChange={event => onChange(event.currentTarget.checked)}
    />
    <span className="min-w-0">
      <strong>{option.label}</strong>
      {option.description && <small>{option.description}</small>}
    </span>
  </label>;
}

export function UserQuestion({ request, onAnswer, onCancel, disabled = false }: UserQuestionProps) {
  const identity = `${request.id}:${request.revision}`;
  const currentIdentity = useRef(identity);
  currentIdentity.current = identity;
  const [form, setForm] = useState<FormState>(() => ({
    identity,
    answers: emptyAnswers(request.payload.questions),
    busy: false,
    error: "",
  }));
  const inFlight = useRef<string | null>(null);
  const lastAction = useRef<LastAction | null>(null);
  const current = form.identity === identity ? form : {
    identity,
    answers: emptyAnswers(request.payload.questions),
    busy: false,
    error: "",
  };
  const terminal = request.status !== "pending";
  const locked = disabled || current.busy || terminal;

  useLayoutEffect(() => {
    currentIdentity.current = identity;
    inFlight.current = null;
    lastAction.current = null;
    setForm({ identity, answers: emptyAnswers(request.payload.questions), busy: false, error: "" });
    // 仅在请求身份（id 与版本）变化时重置；同一请求对象重解析不丢用户输入。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [identity]);

  function patch(update: (state: FormState) => FormState) {
    setForm(previous => update(previous.identity === identity ? previous : current));
  }

  // 同一请求版本且答复内容相同时复用 request_key；内容变化才换新 key。
  function reuseKey(payload: string): string {
    const previous = lastAction.current;
    if (previous && previous.identity === identity && previous.payload === payload) return previous.key;
    const key = crypto.randomUUID();
    lastAction.current = { identity, key, payload };
    return key;
  }

  function changeDraft(questionId: string, update: (draft: Draft) => Draft) {
    if (locked) return;
    patch(state => ({
      ...state,
      answers: { ...state.answers, [questionId]: update(state.answers[questionId] ?? { optionIds: [], text: "" }) },
      error: "",
    }));
  }

  function makeAnswers(): { answers: InteractionAnswer[]; payload: string } | null {
    const answers: InteractionAnswer[] = [];
    for (const question of request.payload.questions) {
      const draft = current.answers[question.id] ?? { optionIds: [], text: "" };
      const text = draft.text.trim();
      if (question.mode === "text") {
        if (!text) return null;
        answers.push({ question_id: question.id, option_ids: [], text });
        continue;
      }
      if (question.mode === "single" && draft.optionIds.length > 1) return null;
      if (!draft.optionIds.length && !text) return null;
      answers.push({
        question_id: question.id,
        option_ids: draft.optionIds,
        text: question.allow_text && text ? text : null,
      });
    }
    return { answers, payload: JSON.stringify(answers) };
  }

  async function submit() {
    if (locked || inFlight.current === identity) return;
    const prepared = makeAnswers();
    if (!prepared) {
      patch(state => ({ ...state, error: "请回答所有问题后再提交" }));
      return;
    }
    inFlight.current = identity;
    patch(state => ({ ...state, busy: true, error: "" }));
    try {
      await onAnswer(request, {
        request_key: reuseKey(prepared.payload),
        expected_revision: request.revision,
        answers: prepared.answers,
        confirmed: null,
      });
    } catch (error) {
      if (currentIdentity.current === identity) {
        patch(state => ({ ...state, error: actionError(error, "提交未完成，请重试") }));
      }
    } finally {
      if (inFlight.current === identity) inFlight.current = null;
      if (currentIdentity.current === identity) {
        setForm(previous => previous.identity === identity ? { ...previous, busy: false } : previous);
      }
    }
  }

  async function cancel() {
    if (locked || inFlight.current === identity) return;
    inFlight.current = identity;
    patch(state => ({ ...state, busy: true, error: "" }));
    try {
      await onCancel(request, {
        request_key: reuseKey(JSON.stringify(["cancel", "user_cancelled"])),
        expected_revision: request.revision,
        reason: "user_cancelled",
      });
    } catch (error) {
      if (currentIdentity.current === identity) {
        patch(state => ({ ...state, error: actionError(error, "取消未完成，请重试") }));
      }
    } finally {
      if (inFlight.current === identity) inFlight.current = null;
      if (currentIdentity.current === identity) {
        setForm(previous => previous.identity === identity ? { ...previous, busy: false } : previous);
      }
    }
  }

  if (terminal) {
    return <section className="space-y-2 rounded-lg border border-border bg-card p-4 text-card-foreground" aria-label="用户问题" data-request-id={request.id}>
      <h3 className="text-sm font-medium">用户问题</h3>
      <p role="status" className="text-sm text-muted-foreground">{terminalLabels[request.status] ?? "请求已结束"}</p>
    </section>;
  }

  return <section className="workbench-interaction interaction-card" data-pending-interaction="true" aria-label="用户问题" data-request-id={request.id}>
    <div className="interaction-head"><CircleHelp aria-hidden="true"/><h3>需要你的回答</h3><span className="obs-pill obs-pill-warn">待回答</span></div>
    {request.payload.questions.map((question, index) => {
      const draft = current.answers[question.id] ?? { optionIds: [], text: "" };
      return <fieldset key={question.id} className="interaction-fieldset" disabled={locked}>
        <legend>{request.payload.questions.length > 1 ? `${index + 1}. ` : ""}{question.prompt}</legend>
        {question.mode === "text" ? <Textarea
          aria-label={question.prompt}
          value={draft.text}
          disabled={locked}
          rows={2}
          onChange={event => changeDraft(question.id, value => ({ ...value, text: event.target.value }))}
        /> : <div className="question-options">
          {question.options.map(option => <QuestionOption
            key={option.id}
            question={question}
            requestId={request.id}
            option={option}
            checked={draft.optionIds.includes(option.id)}
            disabled={locked}
            onChange={checked => changeDraft(question.id, value => ({
              ...value,
              optionIds: question.mode === "single"
                ? checked ? [option.id] : []
                : checked
                  ? [...value.optionIds, option.id]
                  : value.optionIds.filter(id => id !== option.id),
            }))}
          />)}
          {question.allow_text && <label className="block space-y-1 text-sm">
            <span>补充说明（可选）</span>
            <Textarea
              aria-label={`${question.prompt}：补充说明`}
              value={draft.text}
              disabled={locked}
              rows={2}
              onChange={event => changeDraft(question.id, value => ({ ...value, text: event.target.value }))}
            />
          </label>}
        </div>}
      </fieldset>;
    })}
    {current.error && <p role="alert" className="text-sm text-destructive">{current.error}</p>}
    <div className="interaction-actions">
      <Button type="button" disabled={locked} onClick={() => void submit()}>{current.busy ? "提交中…" : "提交回答"}</Button>
      <Button type="button" variant="outline" disabled={locked} onClick={() => void cancel()}>取消问题</Button>
    </div>
  </section>;
}
