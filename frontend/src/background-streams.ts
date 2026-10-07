/** 单一多路订阅与每个 Run 的游标投影；不执行业务/审批，不持久保存正文或 Key。 */
import { ActiveRunView, RunStreamFrame, RunView, consumeMultiplex, requestJson } from "./api.js";

export interface RunSession {
  run: RunView;
  cursor: number;
  step: number;
  draft: string;
  terminal: boolean;
  connected: boolean;
  interactionRuns: Set<string>;
  activity: string[];
}

export class BackgroundStreams {
  private sessions = new Map<string, RunSession>();
  private topics = new Map<string, string>();
  private controller: AbortController | null = null;
  private generation = 0;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private discovery: ReturnType<typeof setInterval> | null = null;
  private polling = false;
  private closed = false;

  private prune(): void {
    for (const [runId, session] of this.sessions) {
      if (this.sessions.size <= 128) break;
      if (session.terminal) {
        this.sessions.delete(runId);
        if (this.topics.get(session.run.topic_id) === runId) this.topics.delete(session.run.topic_id);
      }
    }
  }

  constructor(private changed: (session: RunSession, item?: RunStreamFrame) => void, private failed: (message: string) => void) {}

  forTopic(topicId: string): RunSession | undefined {
    const run = this.topics.get(topicId); return run ? this.sessions.get(run) : undefined;
  }

  all(): RunSession[] { return [...this.sessions.values()]; }
  forRun(runId: string): RunSession | undefined { return this.sessions.get(runId); }

  track(run: RunView): RunSession {
    const existing = this.sessions.get(run.id);
    if (existing) return existing;
    const session: RunSession = { run, cursor: 0, step: 0, draft: "", terminal: false, connected: false, interactionRuns: new Set(), activity: [] };
    const previous = this.topics.get(run.topic_id);
    if (previous && this.sessions.get(previous)?.terminal) this.sessions.delete(previous);
    this.sessions.set(run.id, session); this.topics.set(run.topic_id, run.id);
    this.changed(session); this.restart(); return session;
  }

  async discover(): Promise<void> {
    if (this.polling || this.closed) return;
    this.polling = true;
    try {
      const active = await requestJson<ActiveRunView[]>("/v1/runs/active");
      let added = false;
      for (const view of active) {
        if (!this.sessions.has(view.run.id)) {
          const session: RunSession = { run: view.run, cursor: 0, step: 0, draft: "", terminal: false, connected: false, interactionRuns: new Set(), activity: [] };
          this.sessions.set(view.run.id, session); this.topics.set(view.run.topic_id, view.run.id); this.changed(session); added = true;
        }
      }
      if (added) this.restart();
    } finally { this.polling = false; }
  }

  async start(): Promise<void> {
    this.closed = false; await this.discover();
    if (this.discovery === null) this.discovery = setInterval(() => void this.discover().catch(() => undefined), 1000);
  }

  retry(runId: string): void {
    const session = this.sessions.get(runId);
    if (session && !session.terminal && !session.connected) this.restart();
  }

  close(): void {
    this.closed = true; this.generation += 1; this.controller?.abort();
    if (this.retryTimer) clearTimeout(this.retryTimer);
    if (this.discovery) clearInterval(this.discovery);
    this.retryTimer = this.discovery = null;
  }


  private restart(): void {
    const generation = ++this.generation; this.controller?.abort();
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.retryTimer = null;
    const active = [...this.sessions.values()].filter((session) => !session.terminal);
    if (!active.length || this.closed) return;
    const controller = new AbortController(); this.controller = controller;
    active.forEach((session) => { session.connected = true; this.changed(session); });
    const sources = active.map((session) => ({ run_id: session.run.id, topic_id: session.run.topic_id, next_seq: session.cursor }));
    void consumeMultiplex(sources, (item) => {
      if (generation !== this.generation || controller.signal.aborted) return;
      const session = this.sessions.get(item.frame.stream_id);
      if (!session || item.frame.seq < session.cursor) return;
      if (item.frame.seq !== session.cursor) throw new Error("多路流游标缺口");
      session.cursor += 1;
      const data = item.frame.data;
      if (data?.type === "model_step" && data.phase === "start") { session.step = Number(data.step); session.draft = ""; }
      if (data?.type === "text_delta") session.draft += String(data.text || "");
      if (data?.type === "model_step" && data.phase === "start") session.activity.push(`步骤 ${Number(data.step) + 1}：模型生成`);
      if (data?.type === "tool_call") session.activity.push(`调用工具 ${String(data.name)}`);
      if (data?.type === "tool_result") session.activity.push(`工具 ${String(data.name)}：${data.is_error ? "失败" : "完成"}`);
      if (session.activity.length > 128) session.activity.splice(0, session.activity.length - 128);
      if (data?.type === "step_finish") session.draft = "";
      if (data?.type === "interaction_request") {
        const request = data.interaction as { run_id?: string } | undefined;
        if (request?.run_id) session.interactionRuns.add(request.run_id);
      }
      if (item.frame.kind === "end" || item.frame.kind === "error") {
        session.terminal = true; session.connected = false; session.draft = "";
        session.run = { ...session.run, status: String(data?.status || "failed") as RunView["status"] };
        this.prune();
      }
      this.changed(session, item);
    }, controller.signal).catch((error: unknown) => {
      if (generation !== this.generation || controller.signal.aborted || this.closed) return;
      for (const session of active.filter((value) => !value.terminal)) { session.connected = false; this.changed(session); }
      this.failed(error instanceof Error ? error.message : "后台流读取失败");
      this.retryTimer = setTimeout(() => this.restart(), 1000);
    });
  }
}
