import type { ReactNode } from "react";
import type { Actor } from "../lib/session-contracts";
import type { ObservationsState } from "./use-observations";
import type { ObsState } from "./obs-route";
import { obsStatusNames, type ObsRunStatus } from "./obs-contracts";

/** 观测页共用 props：数据由工作台统一读取后下发，页面本身不重复请求。 */
export interface ObsViewProps {
  apiKey: string;
  workspace: string;
  identity: Actor | null;
  state: ObsState;
  onState: (next: ObsState, replace?: boolean) => void;
  dark: boolean;
  data: ObservationsState;
}

/** 观测页内共用小组件：页头、状态签、空态与提示。样式全部在 obs.css，沿用工作台 token。 */

export type Tone = "ok" | "bad" | "warn" | "neutral";

export function statusTone(status: ObsRunStatus): Tone {
  if (status === "completed") return "ok";
  if (status === "failed" || status === "aborted") return "bad";
  if (status === "interrupted" || status === "awaiting") return "warn";
  return "neutral";
}

const toneText: Record<Tone, string> = { ok: "✓", bad: "×", warn: "−", neutral: "○" };

export function ObsStatusPill({ status }: { status: ObsRunStatus }) {
  const tone = statusTone(status);
  return <span className={`obs-pill obs-pill-${tone}`}><span aria-hidden="true">{toneText[tone]}</span>{obsStatusNames[status]}</span>;
}

export function ObsPill({ tone = "neutral", children }: { tone?: Tone; children: ReactNode }) {
  return <span className={`obs-pill obs-pill-${tone}`}>{children}</span>;
}

export function ObsPageHead({ title, subtitle, actions }: { title: string; subtitle?: string; actions?: ReactNode }) {
  return <header className="obs-page-head">
    <div>
      <h2>{title}</h2>
      {subtitle && <p>{subtitle}</p>}
    </div>
    {actions && <div className="obs-actions">{actions}</div>}
  </header>;
}

export function ObsEmpty({ title, children }: { title: string; children?: ReactNode }) {
  return <div className="obs-empty">
    <h3>{title}</h3>
    {children && <p>{children}</p>}
  </div>;
}

export function ObsNotice({ tone = "warn", title, children }: { tone?: Tone; title: string; children?: ReactNode }) {
  return <div className={`obs-notice obs-notice-${tone}`} role={tone === "bad" ? "alert" : "status"}>
    <div><strong>{title}</strong>{children}</div>
  </div>;
}

export function ObsFact({ label, children }: { label: string; children: ReactNode }) {
  return <div className="obs-fact"><span>{label}</span><strong>{children}</strong></div>;
}

export function ObsScopeNote({ children }: { children: ReactNode }) {
  return <p className="obs-scope-note">{children}</p>;
}
