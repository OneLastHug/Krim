// 账号设置：已批准 HTML 样板（docs/prototypes/accounts）的正式 React 接入。
// 复用项目 Button/Input/Label/Dialog/PopoverMenu；样式集中在 account-settings.css，
// 命名空间 .accounts-settings / .account-*，不改动全局既有控件。
// 数据契约与错误文案来自 ../lib/accounts；结果未知只允许原键查询，不自动重发、不换键重写。
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import type { Actor } from "../lib/session-contracts";
import { RequestError } from "../api";
import {
  accountError,
  accountsApi,
  isAccountOutcomeUnknown,
  newRequestKey,
  type AccountEvent,
  type AccountProfile,
  type AccountStatus,
  type AccountView,
  type KeyView,
  type MembershipRole,
  type PlatformRole,
  type WorkspaceRef,
} from "../lib/accounts";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogTitle } from "./ui/dialog";
import { PopoverMenu } from "./popover-menu";
import {
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Clock,
  Ellipsis,
  KeyRound,
  Lock,
  Plus,
  RotateCw,
  Search,
  TriangleAlert,
  User,
} from "lucide-react";
import "./account-settings.css";

export type AccountTab = "profile" | "accounts" | "events";

export interface AccountSettingsProps {
  apiKey: string;
  identity: Actor | null;
  profile: AccountProfile | null;
  active: boolean;
  tab: AccountTab;
  onTabChange: (tab: AccountTab) => void;
  onProfileChange: () => void;
  /** self rotate 成功拿到 token 时立即回调；App 仅存 ref，不切换当前 key。 */
  onKeyPrepared?: (token: string) => void;
  /** secret modal 关闭时回调；App 此时才把内存 key 换成新密钥。 */
  onKeyChange: (key: string) => void;
  onLogout: () => void;
  gatewayName?: string;
  onConnect: () => void;
}

type ConfirmTarget = {
  principal_id: string;
  display_name: string;
  platform_role: PlatformRole;
  status: AccountStatus;
  revision: number;
  has_active_key: boolean;
  key: KeyView | null;
};

type ConfirmKind = "rotate-self" | "revoke-self" | "issue" | "disable" | "enable" | "revoke";

type SecretState = {
  token: string;
  self: boolean;
  accountName: string;
  heading: string;
  note: string;
};

// 结果未知的处理只有两条路：管理员按原 request_key 查询管理记录，或联系管理员恢复。
// 不携带重发闭包：任何新尝试都必须由用户重新发起操作（服务端按新 request_key 幂等）。
type UnknownState = {
  title: string;
  text: string;
  requestKey: string;
  queryable: boolean;
  info?: string;
  error?: string;
};

const GUARD_TEXT = "至少保留一个可登录的管理员账号。";
const ROLE_TEXT: Record<PlatformRole, string> = { member: "普通账号", platform_admin: "管理员" };
const ACTION_TEXT: Record<string, string> = {
  create: "创建账号",
  update: "更新账号",
  issue: "签发密钥",
  rotate: "轮换密钥",
  revoke: "吊销密钥",
  memberships: "工作区权限",
  recover: "恢复密钥",
  import: "导入账号",
  bootstrap: "初始化账号",
};
const STATUS_FILTER_OPTIONS: [string, string][] = [
  ["all", "全部状态"],
  ["active", "已启用"],
  ["disabled", "已停用"],
];
const PLATFORM_ROLE_OPTIONS: [PlatformRole, string][] = [
  ["member", "普通账号"],
  ["platform_admin", "管理员"],
];
const WORKSPACE_ROLE_OPTIONS: [MembershipRole, string][] = [
  ["viewer", "只读"],
  ["member", "成员"],
  ["admin", "管理员"],
];
const EVENTS_PAGE_SIZE = 20;
const PAGE_SIZES = [5, 10, 20];

const keyState = (account: { key: KeyView | null }): string =>
  !account.key ? "未签发" : account.key.status === "active" ? "有效" : "已吊销";

// 旧导入数据的共享工作区 display_name 可能仍是 workspace_id（如 local）：主界面按个人工作区展示，内部 ID 留给详情。
const workspaceLabel = (item: WorkspaceRef): string =>
  item.display_name === "local" && item.workspace_id === "local" ? "个人工作区" : item.display_name;

const formatTime = (value: number | null): string => {
  if (value === null) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value * 1000));
};

const formatTimeFull = (value: number | null): string =>
  value === null ? "" : new Date(value * 1000).toLocaleString("zh-CN", { hour12: false });

// 明确拒绝的展示文案；guard 场景的 403 是服务端最后管理员保护。
const definiteText = (error: unknown, guard = false): string => {
  if (guard && error instanceof RequestError && error.status === 403) {
    return `${GUARD_TEXT}你可以轮换密钥，但不能停用此账号或吊销其唯一密钥。`;
  }
  return accountError(error);
};

const toTarget = (view: AccountView): ConfirmTarget => ({
  principal_id: view.principal_id,
  display_name: view.display_name,
  platform_role: view.platform_role,
  status: view.status,
  revision: view.revision,
  has_active_key: view.has_active_key,
  key: view.key,
});

function SelectWrap({ label, value, disabled, onChange, options }: {
  label: string;
  value: string;
  disabled?: boolean;
  onChange: (value: string) => void;
  options: readonly (readonly [string, string])[];
}) {
  return (
    <span className="account-select-wrap">
      <select
        aria-label={label}
        value={value}
        disabled={disabled}
        onChange={event => onChange(event.target.value)}
      >
        {options.map(([optionValue, optionText]) => (
          <option key={optionValue} value={optionValue}>{optionText}</option>
        ))}
      </select>
      <ChevronDown aria-hidden="true" />
    </span>
  );
}

function SettingsDialog({ open, onClose, title, description, wide, footer, children }: {
  open: boolean;
  onClose: () => void;
  title: string;
  description?: string;
  wide?: boolean;
  footer?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <Dialog open={open} onOpenChange={value => { if (!value) onClose(); }}>
      <DialogContent className={`account-dialog-content${wide ? " account-dialog-wide" : ""}`}>
        <DialogTitle className="account-dialog-title">{title}</DialogTitle>
        {description ? <DialogDescription className="account-dialog-description">{description}</DialogDescription> : null}
        <div className="account-dialog-body">{children}</div>
        {footer ? <DialogFooter className="account-dialog-footer">{footer}</DialogFooter> : null}
      </DialogContent>
    </Dialog>
  );
}

const StatusBadge = ({ status }: { status: AccountStatus }) => (
  <span className={`account-status${status === "disabled" ? " disabled" : ""}`}>
    {status === "disabled" ? "已停用" : "已启用"}
  </span>
);

function confirmMeta(kind: ConfirmKind, target: ConfirmTarget): { title: string; description: string; label: string; destructive: boolean } {
  if (kind === "rotate-self") {
    return { title: "轮换访问密钥", description: "旧密钥会立即失效。已接受的任务继续运行，其他页面需要用新密钥重新登录。", label: "轮换密钥", destructive: false };
  }
  if (kind === "issue") {
    return target.has_active_key
      ? { title: "重新签发密钥", description: "旧密钥会立即失效，当前账号的排队任务和运行任务将停止。请保存新密钥后再交给账号使用者。", label: "重新签发", destructive: false }
      : { title: "签发访问密钥", description: "密钥用于登录此账号。生成后只显示一次，请妥善保存。", label: "签发密钥", destructive: false };
  }
  if (kind === "disable") {
    return { title: "停用账号", description: "此账号将无法登录，排队任务和运行任务将停止。已有会话和文件会保留。", label: "停用账号", destructive: true };
  }
  if (kind === "enable") {
    return { title: "启用账号", description: "未吊销的密钥可重新登录。之前取消的任务不会自动恢复。", label: "启用账号", destructive: false };
  }
  return { title: "吊销访问密钥", description: "此密钥将立即失效，账号的排队任务和运行任务将停止。重新登录需要管理员签发新密钥。", label: "吊销密钥", destructive: true };
}

export function AccountSettings({
  apiKey,
  identity,
  profile,
  active,
  tab,
  onTabChange,
  onProfileChange,
  onKeyPrepared,
  onKeyChange,
  onLogout,
  gatewayName,
  onConnect,
}: AccountSettingsProps) {
  const admin = profile?.platform_role === "platform_admin";
  const selfId = profile?.principal_id ?? identity?.principal_id ?? "";
  const selfTarget: ConfirmTarget | null = profile
    ? {
      principal_id: profile.principal_id,
      display_name: profile.display_name,
      platform_role: profile.platform_role,
      status: profile.status,
      revision: profile.revision,
      has_active_key: profile.key?.status === "active",
      key: profile.key,
    }
    : null;

  const [toast, setToast] = useState("");
  const [nameDraft, setNameDraft] = useState<string | null>(null);
  const [nameConflict, setNameConflict] = useState(false);
  const [nameFeedback, setNameFeedback] = useState("");
  const [nameError, setNameError] = useState("");
  const [renamePending, setRenamePending] = useState<{ key: string } | null>(null);
  // rename 结果未确认：禁止重提交，直到重新读取资料核对（管理员可原键查询管理记录）。
  const [renameUnconfirmed, setRenameUnconfirmed] = useState<{ requestKey: string; info?: string; error?: string } | null>(null);
  const [renameQueryBusy, setRenameQueryBusy] = useState(false);
  const [saveNameBusy, setSaveNameBusy] = useState(false);
  const [confirm, setConfirm] = useState<{ kind: ConfirmKind; target: ConfirmTarget } | null>(null);
  const [confirmBusy, setConfirmBusy] = useState(false);
  const [confirmError, setConfirmError] = useState("");
  const [secret, setSecret] = useState<SecretState | null>(null);
  const [secretCopied, setSecretCopied] = useState(false);
  const [unknown, setUnknown] = useState<UnknownState | null>(null);
  const [unknownBusy, setUnknownBusy] = useState(false);
  const [list, setList] = useState<{ items: AccountView[]; total: number } | null>(null);
  const [listState, setListState] = useState<"loading" | "ready" | "error">("loading");
  const [queryDraft, setQueryDraft] = useState("");
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<"all" | "active" | "disabled">("all");
  const [pageNumber, setPageNumber] = useState(1);
  const [pageSize, setPageSize] = useState(5);
  const [listRefresh, setListRefresh] = useState(0);
  const [events, setEvents] = useState<{ items: AccountEvent[]; total: number } | null>(null);
  const [eventsState, setEventsState] = useState<"loading" | "ready" | "error">("loading");
  const [eventsPage, setEventsPage] = useState(1);
  const [eventsRefresh, setEventsRefresh] = useState(0);
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createRole, setCreateRole] = useState<PlatformRole>("member");
  const [createError, setCreateError] = useState("");
  const [createBusy, setCreateBusy] = useState(false);
  const [created, setCreated] = useState<AccountView | null>(null);
  const [editId, setEditId] = useState<string | null>(null);
  const [editRefresh, setEditRefresh] = useState(0);
  const [rowMenu, setRowMenu] = useState<{ account: AccountView; anchor: HTMLElement } | null>(null);

  const toastTimer = useRef<number | undefined>(undefined);
  const confirmKeyRef = useRef<{ stamp: string; key: string } | null>(null);
  const createKeyRef = useRef<{ stamp: string; key: string } | null>(null);

  // —— 会话事实（key / principal / generation）变更：清空秘密、modal、列表与全部旧响应 ——
  const sessionKey = `${apiKey}\u0000${selfId}\u0000${profile?.generation ?? identity?.generation ?? 0}`;
  const sessionRef = useRef(sessionKey);
  const resetLocal = useCallback(() => {
    window.clearTimeout(toastTimer.current);
    setToast(""); setSecret(null); setUnknown(null); setConfirm(null); confirmKeyRef.current = null;
    setCreateOpen(false); setCreateName(""); setCreateRole("member"); setCreateError(""); setCreateBusy(false);
    createKeyRef.current = null; setCreated(null); setRowMenu(null);
    setNameDraft(null); setNameConflict(false); setNameFeedback(""); setNameError(""); setRenamePending(null); setSaveNameBusy(false);
    setRenameUnconfirmed(null); setRenameQueryBusy(false);
    setList(null); setListState("loading"); setPageNumber(1); setQueryDraft(""); setQuery(""); setFilter("all");
    setEvents(null); setEventsState("loading"); setEventsPage(1);
    setEditId(null);
  }, []);
  useEffect(() => {
    if (sessionRef.current === sessionKey) return;
    sessionRef.current = sessionKey;
    resetLocal();
  }, [sessionKey, resetLocal]);

  // —— 离开设置页：不保留密钥 modal；self rotate 已完成的 token 必须交回 App ——
  const activeRef = useRef(active);
  useEffect(() => {
    if (active === activeRef.current) return;
    activeRef.current = active;
    if (active) return;
    if (secret) {
      if (secret.self) onKeyChange(secret.token);
      setSecret(null);
    }
  }, [active, secret, onKeyChange]);

  const showToast = useCallback((text: string) => {
    window.clearTimeout(toastTimer.current);
    setToast(text);
    toastTimer.current = window.setTimeout(() => setToast(""), 4200);
  }, []);
  useEffect(() => () => window.clearTimeout(toastTimer.current), []);

  // —— 个人资料：显示名称（expected_revision 乐观锁）——
  useEffect(() => {
    // profile 版本变化（含 focus 外部更新）：有未保存草稿且与服务端名称不同时保留草稿并标记冲突，
    // 不静默覆盖；只有用户主动「重新读取」或草稿与服务端一致（如自己保存成功后的回读）才丢弃。
    const serverName = profile?.display_name ?? "";
    setNameFeedback("");
    setRenameUnconfirmed(null);
    setNameError("");
    if (nameDraft !== null && nameDraft.trim() !== "" && nameDraft !== serverName) {
      setNameConflict(true);
    } else {
      setNameDraft(null);
      setNameConflict(false);
    }
  }, [profile?.revision]);
  const displayNameValue = nameDraft ?? profile?.display_name ?? "";
  const submitRename = async () => {
    if (!profile || saveNameBusy) return;
    const owner = sessionKey;
    const name = displayNameValue.trim();
    if (!name) { setNameError("请输入显示名称。"); return; }
    setNameError(""); setNameFeedback("");
    const key = renamePending?.key ?? newRequestKey();
    setRenamePending({ key });
    setSaveNameBusy(true);
    try {
      await accountsApi.updateProfile(apiKey, { display_name: name, expected_revision: profile.revision }, key);
      if (sessionRef.current !== owner) return;
      setRenamePending(null);
      setNameFeedback("已保存");
      showToast("个人资料已更新。");
      onProfileChange();
    } catch (error) {
      if (sessionRef.current !== owner) return;
      if (error instanceof RequestError && error.status === 409 && (error.code === "revision_conflict" || error.code === "key_conflict")) {
        setRenamePending(null);
        setNameConflict(true);
      } else if (isAccountOutcomeUnknown(error)) {
        // 结果未确认：禁用重提交，只能重新读取资料核对或（管理员）原键查询，避免换 key 自动重复。
        setRenamePending(null);
        setRenameUnconfirmed({ requestKey: key });
      } else {
        setRenamePending(null);
        setNameError(`${accountError(error)}。`);
      }
    } finally {
      if (sessionRef.current === owner) setSaveNameBusy(false);
    }
  };

  // rename 原键查询（管理员）：确认是否已保存；不重发、不换键。
  const queryRenameUnknown = async () => {
    const current = renameUnconfirmed;
    if (!current || !admin || renameQueryBusy) return;
    const owner = sessionKey;
    setRenameQueryBusy(true);
    try {
      const page = await accountsApi.events(apiKey, { offset: 0, limit: 5, request_key: current.requestKey });
      if (sessionRef.current !== owner) return;
      setRenameUnconfirmed(state => state
        ? {
          ...state,
          info: page.items.length > 0
            ? "管理记录中已有该请求的完成结果：资料已保存。请重新读取资料确认最新内容。"
            : "没有找到该请求的完成记录，原操作可能未执行。请重新读取资料后再试。",
          error: undefined,
        }
        : state);
    } catch (error) {
      if (sessionRef.current !== owner) return;
      setRenameUnconfirmed(state => state ? { ...state, info: undefined, error: `${accountError(error)}。` } : state);
    } finally {
      if (sessionRef.current === owner) setRenameQueryBusy(false);
    }
  };

  const refreshAccounts = useCallback(() => setListRefresh(value => value + 1), []);
  const mergeAccount = useCallback((account: AccountView) => {
    setList(current => current
      ? { ...current, items: current.items.map(item => item.principal_id === account.principal_id ? account : item) }
      : current);
  }, []);
  const bumpEdit = (principalId: string) => {
    setEditId(current => {
      if (current === principalId) setEditRefresh(value => value + 1);
      return current;
    });
  };

  const finishConfirm = () => {
    setConfirm(null);
    confirmKeyRef.current = null;
    setUnknown(null);
  };

  const runRotateSelf = async (key: string) => {
    const owner = sessionKey;
    const result = await accountsApi.rotate(apiKey, key);
    if (sessionRef.current !== owner) return;
    finishConfirm();
    if (result.token) {
      onKeyPrepared?.(result.token);
      setSecretCopied(false);
      setSecret({
        token: result.token,
        self: true,
        accountName: profile?.display_name ?? "当前账号",
        heading: "新密钥已生成",
        note: "旧密钥已失效，已接受的任务不受影响。关闭前请先保存密钥。",
      });
    } else {
      setUnknown({
        title: "密钥已在此前轮换",
        text: "原轮换请求已完成，但密钥明文只在完成时显示一次。若没有保存新密钥，请联系管理员重新签发以恢复登录。",
        requestKey: key,
        queryable: false,
      });
    }
  };
  const runRevokeSelf = async (key: string) => {
    const owner = sessionKey;
    await accountsApi.revokeSelf(apiKey, key);
    if (sessionRef.current !== owner) return;
    finishConfirm();
    onLogout();
  };
  // 对自己签发/重签：服务端会吊销当前唯一活跃密钥，必须走 self 协议——
  // token 先交 App 暂存（onKeyPrepared），modal 关闭才切换（onKeyChange），并跳过旧 key 的列表刷新。
  const runSelfIssue = async (key: string) => {
    const owner = sessionKey;
    const result = await accountsApi.issue(apiKey, selfId, key);
    if (sessionRef.current !== owner) return;
    finishConfirm();
    if (result.token) {
      onKeyPrepared?.(result.token);
      setSecretCopied(false);
      setSecret({
        token: result.token,
        self: true,
        accountName: profile?.display_name ?? "当前账号",
        heading: "新密钥已签发",
        note: "旧密钥已失效，已接受的任务不受影响。关闭前请先保存密钥。",
      });
    } else {
      setUnknown({
        title: "密钥已在此前签发",
        text: "原签发请求已完成，但密钥明文只在完成时显示一次。请使用已保存的新密钥重新登录；若没有保存，可再次签发并保存新密钥。",
        requestKey: key,
        queryable: admin,
      });
    }
  };
  const runIssue = async (target: ConfirmTarget, key: string) => {
    const owner = sessionKey;
    const result = await accountsApi.issue(apiKey, target.principal_id, key);
    if (sessionRef.current !== owner) return;
    finishConfirm();
    refreshAccounts();
    bumpEdit(target.principal_id);
    if (result.token) {
      setSecretCopied(false);
      setSecret({
        token: result.token,
        self: false,
        accountName: target.display_name,
        heading: "密钥已签发",
        note: "只将密钥交给此账号的使用者。",
      });
    } else {
      setUnknown({
        title: "密钥已在此前签发",
        text: "原签发请求已完成，但密钥明文只在完成时显示一次。若使用者没有保存，可重新签发并保存新密钥。",
        requestKey: key,
        queryable: false,
      });
    }
  };
  const runSetStatus = async (target: ConfirmTarget, status: AccountStatus, key: string) => {
    const owner = sessionKey;
    const result = await accountsApi.update(apiKey, target.principal_id, {
      status,
      expected_revision: target.revision,
    }, key);
    if (sessionRef.current !== owner) return;
    finishConfirm();
    // 自己被停用：账号已不可登录，立即登出而不是继续用失效身份请求。
    if (target.principal_id === selfId && status === "disabled") {
      onLogout();
      return;
    }
    mergeAccount(result.account);
    showToast(status === "active" ? "账号已启用。已取消的任务不会自动恢复。" : "账号已停用，已有会话和文件已保留。");
  };
  const runRevokeKey = async (target: ConfirmTarget, key: string) => {
    // 自己的密钥吊销与 revoke-self 同协议：登出，不再以失效 key 发任何请求。
    if (target.principal_id === selfId) {
      await runRevokeSelf(key);
      return;
    }
    const owner = sessionKey;
    await accountsApi.revoke(apiKey, target.principal_id, key);
    if (sessionRef.current !== owner) return;
    finishConfirm();
    refreshAccounts();
    bumpEdit(target.principal_id);
    showToast("密钥已吊销。");
  };

  const openConfirm = (kind: ConfirmKind, target: ConfirmTarget) => {
    confirmKeyRef.current = null;
    setConfirmError("");
    setConfirm({ kind, target });
  };
  const performConfirm = async () => {
    const state = confirm;
    if (!state || confirmBusy) return;
    const owner = sessionKey;
    const { kind, target } = state;
    let run: (key: string) => Promise<void>;
    let stamp: string;
    if (kind === "rotate-self") { run = runRotateSelf; stamp = "self-rotate"; }
    else if (kind === "revoke-self") { run = runRevokeSelf; stamp = "self-revoke"; }
    else if (kind === "issue" && target.principal_id === selfId) { run = runSelfIssue; stamp = `issue:${target.principal_id}`; }
    else if (kind === "issue") { run = key => runIssue(target, key); stamp = `issue:${target.principal_id}`; }
    else if (kind === "revoke") { run = key => runRevokeKey(target, key); stamp = `revoke:${target.principal_id}`; }
    else {
      const status: AccountStatus = kind === "disable" ? "disabled" : "active";
      run = key => runSetStatus(target, status, key);
      stamp = `${kind}:${target.principal_id}:${target.revision}`;
    }
    const existing = confirmKeyRef.current;
    const key = existing && existing.stamp === stamp ? existing.key : newRequestKey();
    confirmKeyRef.current = { stamp, key };
    setConfirmBusy(true);
    setConfirmError("");
    try {
      await run(key);
    } catch (error) {
      if (sessionRef.current !== owner) return;
      if (isAccountOutcomeUnknown(error)) {
        confirmKeyRef.current = null;
        setConfirm(null);
        setUnknown({
          title: "操作结果尚未确认",
          text: kind === "rotate-self"
            ? "连接中断，暂时无法确认这次轮换是否完成。若已完成，新密钥明文将无法再次显示，需要管理员重新签发恢复登录。"
            : kind === "issue"
              ? "连接中断，暂时无法确认这次密钥签发是否完成。请查询原操作结果，避免重复签发。"
              : "连接中断，暂时无法确认这次操作是否完成。可用原请求标识查询管理记录确认结果。",
          requestKey: key,
          queryable: admin,
        });
      } else {
        setConfirmError(definiteText(error, kind === "disable" || kind === "revoke" || kind === "revoke-self"));
      }
    } finally {
      if (sessionRef.current === owner) setConfirmBusy(false);
    }
  };

  // —— 结果未知的原键查询：管理员按 request_key 查 account-events；不重发、不换键 ——
  const queryUnknown = async () => {
    const current = unknown;
    if (!current || !current.queryable || unknownBusy) return;
    const owner = sessionKey;
    setUnknownBusy(true);
    try {
      const page = await accountsApi.events(apiKey, { offset: 0, limit: 5, request_key: current.requestKey });
      if (sessionRef.current !== owner) return;
      setUnknown(state => state
        ? {
          ...state,
          title: page.items.length > 0 ? "原操作已完成" : state.title,
          text: page.items.length > 0 ? "已核实原请求，不需要重复提交。" : state.text,
          info: page.items.length > 0
            ? (page.items.some(item => ["issue", "rotate", "recover"].includes(item.action))
              ? "密钥明文只在首次签发时显示，查询结果不会再次返回明文。"
              : "管理记录已确认该操作完成。")
            : "没有找到该请求的完成记录，请稍后用原请求标识继续查询，不要重复提交。",
          error: undefined,
        }
        : state);
    } catch (error) {
      if (sessionRef.current !== owner) return;
      setUnknown(state => state ? { ...state, info: undefined, error: `${accountError(error)}。` } : state);
    } finally {
      if (sessionRef.current === owner) setUnknownBusy(false);
    }
  };

  const closeSecret = () => {
    // self rotate：关闭即把新密钥交回 App 内存；旧密钥已失效，App 不能继续用旧 key
    if (secret?.self) onKeyChange(secret.token);
    setSecret(null);
  };
  const copySecret = async () => {
    if (!secret) return;
    try {
      await navigator.clipboard.writeText(secret.token);
      setSecretCopied(true);
      showToast("密钥已复制。");
    } catch {
      showToast("复制未成功，请选择并复制密钥。");
    }
  };

  useEffect(() => {
    const timer = window.setTimeout(() => setQuery(queryDraft.trim()), 250);
    return () => window.clearTimeout(timer);
  }, [queryDraft]);
  useEffect(() => {
    if (!active || tab !== "accounts" || !admin) return;
    const controller = new AbortController();
    setListState("loading");
    void accountsApi.list(apiKey, {
      offset: (pageNumber - 1) * pageSize,
      limit: pageSize,
      status: filter === "all" ? undefined : filter,
      query: query || undefined,
    }, controller.signal).then(value => {
      if (controller.signal.aborted || sessionRef.current !== sessionKey) return;
      setList({ items: value.items, total: value.total });
      setListState("ready");
    }).catch(() => {
      if (controller.signal.aborted || sessionRef.current !== sessionKey) return;
      setListState("error");
    });
    return () => controller.abort();
  }, [active, tab, admin, apiKey, sessionKey, pageNumber, pageSize, filter, query, listRefresh]);
  useEffect(() => {
    if (list && pageNumber > 1 && (pageNumber - 1) * pageSize >= list.total) {
      setPageNumber(Math.max(1, Math.ceil(list.total / pageSize)));
    }
  }, [list, pageNumber, pageSize]);
  useEffect(() => {
    if (!active || tab !== "events" || !admin) return;
    const controller = new AbortController();
    setEventsState("loading");
    void accountsApi.events(apiKey, {
      offset: (eventsPage - 1) * EVENTS_PAGE_SIZE,
      limit: EVENTS_PAGE_SIZE,
    }, controller.signal).then(value => {
      if (controller.signal.aborted || sessionRef.current !== sessionKey) return;
      setEvents({ items: value.items, total: value.total });
      setEventsState("ready");
    }).catch(() => {
      if (controller.signal.aborted || sessionRef.current !== sessionKey) return;
      setEventsState("error");
    });
    return () => controller.abort();
  }, [active, tab, admin, apiKey, sessionKey, eventsPage, eventsRefresh]);

  const openCreate = () => {
    setCreateError("");
    setCreateName("");
    setCreateRole("member");
    createKeyRef.current = null;
    setCreateOpen(true);
  };
  const runCreate = async (name: string, role: PlatformRole, key: string): Promise<void> => {
    const owner = sessionKey;
    const result = await accountsApi.create(apiKey, { display_name: name, platform_role: role }, key);
    if (sessionRef.current !== owner) return;
    setCreateOpen(false);
    createKeyRef.current = null;
    setUnknown(null);
    refreshAccounts();
    setCreated(result.account);
  };
  const submitCreate = async () => {
    if (createBusy) return;
    const owner = sessionKey;
    const name = createName.trim();
    if (!name) { setCreateError("请输入显示名称。"); return; }
    setCreateError("");
    setCreateBusy(true);
    const stamp = `create:${name}:${createRole}`;
    const existing = createKeyRef.current;
    const key = existing && existing.stamp === stamp ? existing.key : newRequestKey();
    createKeyRef.current = { stamp, key };
    try {
      await runCreate(name, createRole, key);
    } catch (error) {
      if (sessionRef.current !== owner) return;
      if (isAccountOutcomeUnknown(error)) {
        setCreateOpen(false);
        setUnknown({
          title: "创建结果尚未确认",
          text: "连接中断，暂时无法确认这次创建是否完成。可用原请求标识查询管理记录确认结果。",
          requestKey: key,
          queryable: admin,
        });
      } else {
        setCreateError(`${accountError(error)}。`);
      }
    } finally {
      if (sessionRef.current === owner) setCreateBusy(false);
    }
  };

  if (!active) return null;

  const blockedView = (
    <div className="account-blocked">
      <Lock aria-hidden="true" />
      <h2>没有账号管理权限</h2>
      <p>当前账号可以管理个人资料和访问密钥。管理其他账号需要平台管理员权限。</p>
      <Button variant="outline" type="button" onClick={() => onTabChange("profile")}>返回个人资料</Button>
    </div>
  );
  const listStart = list ? (pageNumber - 1) * pageSize : 0;
  const listEnd = list ? Math.min(listStart + pageSize, list.total) : 0;
  const eventsStart = events ? (eventsPage - 1) * EVENTS_PAGE_SIZE : 0;
  const eventsEnd = events ? Math.min(eventsStart + EVENTS_PAGE_SIZE, events.total) : 0;

  return (
    <div className="accounts-settings">
      <nav className="account-tabs" aria-label="账号设置">
        <button type="button" className="account-tab" aria-current={tab === "profile" ? "page" : undefined} onClick={() => onTabChange("profile")}>个人资料</button>
        {admin && <button type="button" className="account-tab" aria-current={tab === "accounts" ? "page" : undefined} onClick={() => onTabChange("accounts")}>账号管理</button>}
        {admin && <button type="button" className="account-tab" aria-current={tab === "events" ? "page" : undefined} onClick={() => onTabChange("events")}>管理记录</button>}
      </nav>

      {tab === "profile" && (profile ? (
        <>
          <div className="account-heading">
            <div>
              <h2>个人资料</h2>
              <p>管理你在 Krim 中的名称和访问密钥。</p>
            </div>
          </div>
          <div className="account-field account-profile-name">
            <Label htmlFor="account-display-name">显示名称</Label>
            <Input
              id="account-display-name"
              className="account-input"
              value={displayNameValue}
              maxLength={64}
              autoComplete="off"
              onChange={event => { setNameDraft(event.target.value); setNameConflict(false); setNameFeedback(""); setRenamePending(null); setNameError(""); }}
              onKeyDown={event => { if (event.key === "Enter") { event.preventDefault(); void submitRename(); } }}
            />
            <p className="account-hint">这会显示在账号菜单和管理列表中。</p>
          </div>
          {nameConflict && (
            <div className="account-notice account-notice-warning account-notice-row" role="alert">
              <span>资料已在其他页面更新。重新读取后再保存。</span>
              <Button variant="outline" type="button" onClick={() => { setNameDraft(null); setNameConflict(false); onProfileChange(); }}>重新读取</Button>
            </div>
          )}
          {renameUnconfirmed && (
            <div className="account-notice account-notice-warning" role="alert">
              <Clock aria-hidden="true" />上次保存的结果尚未确认，重新保存已暂时禁用。
              <div className="account-unknown-actions">
                <Button variant="outline" type="button" onClick={onProfileChange}>重新读取资料</Button>
                {admin && <Button variant="ghost" type="button" disabled={renameQueryBusy} onClick={() => void queryRenameUnknown()}>查询原操作结果</Button>}
              </div>
              <p className="account-hint">请先核对资料是否已保存：重新读取后若名称未变即可再次保存。{admin ? "" : "若仍有疑问，请联系管理员按请求标识核对。"}</p>
              <p className="account-hint">请求标识 <span className="account-mono">{renameUnconfirmed.requestKey}</span></p>
              {renameUnconfirmed.info ? <p className="account-hint">{renameUnconfirmed.info}</p> : null}
              {renameUnconfirmed.error ? <p className="account-inline-error">{renameUnconfirmed.error}</p> : null}
            </div>
          )}
          {nameError ? <p className="account-inline-error" role="alert">{nameError}</p> : null}
          <div className="account-form-actions account-save-row">
            <Button type="button" disabled={saveNameBusy || renameUnconfirmed !== null} onClick={() => void submitRename()}>保存更改</Button>
            {nameFeedback ? <span className="account-hint" role="status">{nameFeedback}</span> : null}
          </div>

          <section className="account-section">
            <h3>当前接入</h3>
            <dl className="account-detail-list">
              <div className="account-detail-row"><dt>接入方式</dt><dd>{gatewayName ?? "API 网关"}</dd></div>
              <div className="account-detail-row"><dt>工作区</dt><dd title={profile.workspaces.map(item => workspaceLabel(item)).join("、")}>{profile.workspaces.map(item => workspaceLabel(item)).join("、") || "—"}</dd></div>
              <div className="account-detail-row"><dt>平台角色</dt><dd>{ROLE_TEXT[profile.platform_role]}</dd></div>
            </dl>
            <div className="account-key-actions">
              <Button variant="ghost" type="button" onClick={onConnect}>切换账号</Button>
            </div>
          </section>

          <section className="account-section">
            <h3>访问密钥</h3>
            <p className="account-section-description">用于登录此账号。轮换后，旧密钥将立即失效。</p>
            <dl className="account-detail-list">
              <div className="account-detail-row"><dt>密钥标识</dt><dd className="account-mono">{profile.key?.masked ?? "尚未签发"}</dd></div>
              <div className="account-detail-row"><dt>最近使用</dt><dd title={formatTimeFull(profile.key?.last_used_at ?? null)}>{formatTime(profile.key?.last_used_at ?? null)}</dd></div>
              <div className="account-detail-row"><dt>状态</dt><dd>{keyState(profile)}</dd></div>
            </dl>
            {selfTarget && (
              <div className="account-key-actions">
                {profile.key?.status === "active" && <Button variant="outline" type="button" onClick={() => openConfirm("rotate-self", selfTarget)}>轮换密钥</Button>}
                {profile.key !== null && <Button variant="ghost" className="account-danger-action" type="button" onClick={() => openConfirm("revoke-self", selfTarget)}>吊销密钥</Button>}
              </div>
            )}
            {profile.key?.status === "revoked" && <p className="account-hint">当前密钥已吊销。若持有新的有效密钥，可切换账号重新进入。</p>}
            <details className="account-metadata">
              <summary>账号信息</summary>
              <dl className="account-detail-list">
                <div className="account-detail-row"><dt>账号 ID</dt><dd className="account-mono">{profile.principal_id}</dd></div>
              </dl>
            </details>
          </section>
        </>
      ) : (
        <div className="account-empty" aria-busy="true">
          <p>正在读取账号资料。</p>
        </div>
      ))}

      {tab === "accounts" && (!admin ? (profile ? blockedView : null) : (
        <>
          <div className="account-heading">
            <div>
              <h2>账号管理</h2>
              <p>管理账号、访问密钥和工作区权限。</p>
            </div>
            <Button type="button" onClick={openCreate}><Plus aria-hidden="true" />新建账号</Button>
          </div>
          <div className="account-search-filter">
            <label className="account-search">
              <Search aria-hidden="true" />
              <Input
                type="search"
                className="account-search-input"
                aria-label="搜索账号"
                placeholder="搜索账号"
                value={queryDraft}
                autoComplete="off"
                onChange={event => { setQueryDraft(event.target.value); setPageNumber(1); }}
              />
            </label>
            <SelectWrap
              label="筛选账号状态"
              value={filter}
              onChange={value => { setFilter(value as "all" | "active" | "disabled"); setPageNumber(1); }}
              options={STATUS_FILTER_OPTIONS}
            />
          </div>
          {listState === "loading" && (
            <div aria-busy="true" aria-label="正在读取账号列表">
              {[0, 1, 2, 3, 4].map(row => (
                <div className="account-skeleton-row" key={row}>
                  <span className="account-skeleton wide" /><span className="account-skeleton" /><span className="account-skeleton" />
                </div>
              ))}
            </div>
          )}
          {listState === "error" && (
            <div className="account-empty" role="alert">
              <TriangleAlert aria-hidden="true" />
              <h3>无法读取账号列表</h3>
              <p>检查连接后重试。已保存的账号不会受到影响。</p>
              <Button variant="outline" type="button" onClick={refreshAccounts}>重试</Button>
            </div>
          )}
          {listState === "ready" && list && list.total === 0 && (
            <div className="account-empty">
              <Search aria-hidden="true" />
              <h3>没有匹配的账号</h3>
              <p>试试其他名称，或清除当前筛选。</p>
              <Button variant="outline" type="button" onClick={() => { setQueryDraft(""); setQuery(""); setFilter("all"); setPageNumber(1); }}>清除筛选</Button>
            </div>
          )}
          {listState === "ready" && list && list.total > 0 && (
            <>
              <table className="account-table">
                <colgroup>
                  <col style={{ width: "34%" }} /><col style={{ width: "21%" }} /><col style={{ width: "19%" }} /><col style={{ width: "19%" }} /><col style={{ width: "7%" }} />
                </colgroup>
                <thead>
                  <tr>
                    <th scope="col">账号</th>
                    <th scope="col" className="account-role-column">平台角色</th>
                    <th scope="col" className="account-status-column">状态</th>
                    <th scope="col" className="account-key-column">访问密钥</th>
                    <th scope="col" className="account-action-column"><span className="sr-only">操作</span></th>
                  </tr>
                </thead>
                <tbody>
                  {list.items.map(item => (
                    <tr key={item.principal_id}>
                      <td>
                        <button type="button" className="account-cell" onClick={() => { setRowMenu(null); setEditId(item.principal_id); }} aria-label={`查看 ${item.display_name}`}>
                          <span className="account-avatar" aria-hidden="true">{item.display_name.charAt(0)}</span>
                          <span className="account-name-wrap">
                            <span className="account-name" title={item.display_name}>
                              {item.display_name}
                              {item.principal_id === selfId && <span className="account-self-label">你</span>}
                            </span>
                            {!item.has_active_key && <span className="account-subline">等待签发密钥</span>}
                          </span>
                        </button>
                      </td>
                      <td className="account-role-column">{ROLE_TEXT[item.platform_role]}</td>
                      <td className="account-status-column"><StatusBadge status={item.status} /></td>
                      <td className="account-key-column"><span className="account-key-state">{keyState(item)}</span></td>
                      <td className="account-row-actions account-action-column">
                        <Button
                          variant="ghost"
                          size="icon-sm"
                          type="button"
                          aria-label={`${item.display_name}的操作`}
                          aria-haspopup="menu"
                          aria-expanded={rowMenu?.account.principal_id === item.principal_id}
                          onClick={event => {
                            const anchor = event.currentTarget;
                            setRowMenu(current => current?.account.principal_id === item.principal_id ? null : { account: item, anchor });
                          }}
                        >
                          <Ellipsis aria-hidden="true" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="account-pagination">
                <span>第 {listStart + 1}–{listEnd} 项，共 {list.total} 个账号</span>
                <div className="account-pagination-controls">
                  <SelectWrap
                    label="每页账号数"
                    value={String(pageSize)}
                    onChange={value => { setPageSize(Number(value)); setPageNumber(1); }}
                    options={PAGE_SIZES.map(size => [String(size), `每页 ${size} 项`] as [string, string])}
                  />
                  <Button variant="ghost" size="icon-sm" type="button" aria-label="上一页" disabled={pageNumber === 1} onClick={() => setPageNumber(value => Math.max(1, value - 1))}><ChevronLeft aria-hidden="true" /></Button>
                  <span className="account-page-number" aria-label={`第 ${pageNumber} 页`}>{pageNumber}</span>
                  <Button variant="ghost" size="icon-sm" type="button" aria-label="下一页" disabled={listEnd >= list.total} onClick={() => setPageNumber(value => value + 1)}><ChevronRight aria-hidden="true" /></Button>
                </div>
              </div>
            </>
          )}
          <PopoverMenu
            open={rowMenu !== null}
            anchor={rowMenu?.anchor ?? null}
            placement="below"
            className="account-row-menu"
            label={rowMenu ? `${rowMenu.account.display_name}的操作` : "账号操作"}
            onClose={() => setRowMenu(null)}
          >
            {rowMenu && (
              <>
                <button type="button" role="menuitem" className="account-menu-item" onClick={() => { const id = rowMenu.account.principal_id; setRowMenu(null); setEditId(id); }}>
                  <User aria-hidden="true" />查看账号
                </button>
                <button type="button" role="menuitem" className="account-menu-item" onClick={() => { const target = toTarget(rowMenu.account); setRowMenu(null); openConfirm("issue", target); }}>
                  <KeyRound aria-hidden="true" />{rowMenu.account.key?.status === "active" ? "重新签发密钥" : "签发密钥"}
                </button>
                <hr className="account-menu-separator" />
                <button
                  type="button"
                  role="menuitem"
                  className={`account-menu-item${rowMenu.account.status === "active" ? " account-danger-item" : ""}`}
                  onClick={() => {
                    const target = toTarget(rowMenu.account);
                    setRowMenu(null);
                    openConfirm(target.status === "active" ? "disable" : "enable", target);
                  }}
                >
                  {rowMenu.account.status === "active" ? <><Lock aria-hidden="true" />停用账号</> : <><RotateCw aria-hidden="true" />启用账号</>}
                </button>
              </>
            )}
          </PopoverMenu>
        </>
      ))}

      {tab === "events" && (!admin ? (profile ? blockedView : null) : (
        <>
          <div className="account-heading">
            <div>
              <h2>管理记录</h2>
              <p>查看账号和密钥的管理操作。</p>
            </div>
          </div>
          {eventsState === "loading" && (
            <div aria-busy="true" aria-label="正在读取管理记录">
              {[0, 1, 2, 3].map(row => (
                <div className="account-skeleton-row" key={row}>
                  <span className="account-skeleton wide" /><span className="account-skeleton" /><span className="account-skeleton" />
                </div>
              ))}
            </div>
          )}
          {eventsState === "error" && (
            <div className="account-empty" role="alert">
              <TriangleAlert aria-hidden="true" />
              <h3>无法读取管理记录</h3>
              <p>检查连接后重试。</p>
              <Button variant="outline" type="button" onClick={() => setEventsRefresh(value => value + 1)}>重试</Button>
            </div>
          )}
          {eventsState === "ready" && events && events.total === 0 && (
            <div className="account-empty">
              <Clock aria-hidden="true" />
              <h3>还没有管理记录</h3>
              <p>创建账号、签发密钥等操作会记录在这里。</p>
            </div>
          )}
          {eventsState === "ready" && events && events.total > 0 && (
            <>
              <table className="account-table account-table-audit">
                <thead>
                  <tr>
                    <th scope="col">操作</th>
                    <th scope="col">账号</th>
                    <th scope="col">操作者</th>
                    <th scope="col">时间</th>
                  </tr>
                </thead>
                <tbody>
                  {events.items.map(item => (
                    <tr key={item.id}>
                      <td>{ACTION_TEXT[item.action] ?? item.action}</td>
                      <td className="account-name" title={item.target ?? ""}>{item.target ?? "—"}</td>
                      <td>{item.actor}</td>
                      <td className="account-muted" title={formatTimeFull(item.at)}>{formatTime(item.at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="account-pagination">
                <span>第 {eventsStart + 1}–{eventsEnd} 项，共 {events.total} 条记录</span>
                <div className="account-pagination-controls">
                  <Button variant="ghost" size="icon-sm" type="button" aria-label="上一页" disabled={eventsPage === 1} onClick={() => setEventsPage(value => Math.max(1, value - 1))}><ChevronLeft aria-hidden="true" /></Button>
                  <span className="account-page-number" aria-label={`第 ${eventsPage} 页`}>{eventsPage}</span>
                  <Button variant="ghost" size="icon-sm" type="button" aria-label="下一页" disabled={eventsEnd >= events.total} onClick={() => setEventsPage(value => value + 1)}><ChevronRight aria-hidden="true" /></Button>
                </div>
              </div>
            </>
          )}
        </>
      ))}

      {editId && (
        <EditAccountDialog
          apiKey={apiKey}
          principalId={editId}
          selfEdit={editId === selfId}
          refreshSignal={editRefresh}
          onClose={() => setEditId(null)}
          onSaved={mergeAccount}
          onProfileChange={onProfileChange}
          // 从详情发起签发/吊销时先关闭详情，避免确认框与详情双 modal 堆叠抢焦点。
          onIssue={target => { setEditId(null); openConfirm("issue", target); }}
          onRevoke={target => { setEditId(null); openConfirm("revoke", target); }}
          showToast={showToast}
        />
      )}

      {confirm && (() => {
        const meta = confirmMeta(confirm.kind, confirm.target);
        const closeConfirm = () => { setConfirm(null); confirmKeyRef.current = null; setConfirmError(""); };
        return (
          <SettingsDialog
            open
            onClose={closeConfirm}
            title={meta.title}
            description={meta.description}
            footer={<>
              <Button variant="outline" type="button" onClick={closeConfirm}>取消</Button>
              <Button variant={meta.destructive ? "destructive" : "default"} type="button" disabled={confirmBusy} onClick={() => void performConfirm()}>{confirmBusy ? "正在提交…" : meta.label}</Button>
            </>}
          >
            <div className="account-confirm-target">
              <span className="account-avatar" aria-hidden="true">{confirm.target.display_name.charAt(0)}</span>
              <div>
                <strong>{confirm.target.display_name}</strong>
                <p className="account-muted">{ROLE_TEXT[confirm.target.platform_role]}</p>
              </div>
            </div>
            {confirmError ? <p className="account-inline-error" role="alert">{confirmError}</p> : null}
          </SettingsDialog>
        );
      })()}

      <SettingsDialog
        open={secret !== null}
        onClose={closeSecret}
        title={secret?.heading ?? ""}
        description={`请保存 ${secret?.accountName ?? ""} 的访问密钥。关闭后无法再次查看明文。`}
        footer={<>
          <Button variant="outline" type="button" onClick={() => void copySecret()}>{secretCopied ? <><Check aria-hidden="true" />已复制</> : "复制密钥"}</Button>
          <Button type="button" onClick={closeSecret}>已保存，关闭</Button>
        </>}
      >
        {secret && (
          <>
            <div className="account-key-value" aria-label="一次性访问密钥">{secret.token}</div>
            <p className="account-small-paragraph">{secret.note}</p>
          </>
        )}
      </SettingsDialog>

      <SettingsDialog
        open={unknown !== null}
        onClose={() => setUnknown(null)}
        title={unknown?.title ?? ""}
        footer={<>
          <Button variant="outline" type="button" onClick={() => setUnknown(null)}>关闭</Button>
          {unknown?.queryable && <Button type="button" disabled={unknownBusy} onClick={() => void queryUnknown()}>查询原操作结果</Button>}
        </>}
      >
        {unknown && (
          <>
            {!unknown.info && <div className="account-notice"><Clock aria-hidden="true" />正在等待管理操作的最终结果。</div>}
            <p className="account-small-paragraph">{unknown.text}</p>
            {unknown.info ? <div className="account-notice">{unknown.info}</div> : null}
            {unknown.error ? <p className="account-inline-error" role="alert">{unknown.error}</p> : null}
            <details className="account-metadata">
              <summary>操作信息</summary>
              <dl className="account-detail-list">
                <div className="account-detail-row"><dt>请求标识</dt><dd className="account-mono">{unknown.requestKey}</dd></div>
              </dl>
            </details>
          </>
        )}
      </SettingsDialog>

      <SettingsDialog
        open={createOpen}
        onClose={() => { setCreateOpen(false); createKeyRef.current = null; }}
        title="新建账号"
        description="新账号会获得独立的个人工作区。创建后可为其签发访问密钥。"
        footer={<>
          <Button variant="outline" type="button" onClick={() => { setCreateOpen(false); createKeyRef.current = null; }}>取消</Button>
          <Button type="button" disabled={createBusy} onClick={() => void submitCreate()}>{createBusy ? "正在创建…" : "创建账号"}</Button>
        </>}
      >
        <div className="account-field">
          <Label htmlFor="account-create-name">显示名称</Label>
          <Input
            id="account-create-name"
            className="account-input"
            value={createName}
            maxLength={64}
            placeholder="例如：项目体验账号"
            autoComplete="off"
            onChange={event => setCreateName(event.target.value)}
            onKeyDown={event => { if (event.key === "Enter") { event.preventDefault(); void submitCreate(); } }}
          />
        </div>
        <div className="account-field">
          <Label htmlFor="account-create-role">平台角色</Label>
          <SelectWrap
            label="平台角色"
            value={createRole}
            onChange={value => setCreateRole(value as PlatformRole)}
            options={PLATFORM_ROLE_OPTIONS}
          />
          <p className="account-hint">管理员可以管理账号，但不会自动获得他人文件的访问权限。</p>
        </div>
        {createError ? <p className="account-inline-error" role="alert">{createError}</p> : null}
      </SettingsDialog>

      <SettingsDialog
        open={created !== null}
        onClose={() => setCreated(null)}
        title="账号已创建"
        description={`${created?.display_name ?? ""} 已获得独立的个人工作区。签发访问密钥后即可登录。`}
        footer={<>
          <Button variant="outline" type="button" onClick={() => setCreated(null)}>稍后签发</Button>
          <Button type="button" onClick={() => { const target = created ? toTarget(created) : null; setCreated(null); if (target) openConfirm("issue", target); }}>签发密钥</Button>
        </>}
      />

      {toast ? <div className="account-toast" role="status"><Check aria-hidden="true" /><span>{toast}</span></div> : null}
    </div>
  );
}

// 账号详情：资料（expected_revision）与工作区权限（expected_generation）两步保存，
// 已完成步骤明确报告且不重发；结果未知按原 request_key 查询管理记录，不换键重写。
// 编辑自己账号并发生角色/状态变更时，服务端会递增授权代次：保存成功即触发 onProfileChange，
// 由 App 先重读 /v2/me 再读资料，避免用过期身份核对。
function EditAccountDialog({ apiKey, principalId, selfEdit, refreshSignal, onClose, onSaved, onProfileChange, onIssue, onRevoke, showToast }: {
  apiKey: string;
  principalId: string;
  selfEdit: boolean;
  refreshSignal: number;
  onClose: () => void;
  onSaved: (account: AccountView) => void;
  onProfileChange: () => void;
  onIssue: (target: ConfirmTarget) => void;
  onRevoke: (target: ConfirmTarget) => void;
  showToast: (text: string) => void;
}) {
  const [detail, setDetail] = useState<AccountView | null>(null);
  const [memberships, setMemberships] = useState<{ generation: number; items: WorkspaceRef[] } | null>(null);
  const [loadError, setLoadError] = useState("");
  const [membershipsError, setMembershipsError] = useState("");
  const [name, setName] = useState("");
  const [role, setRole] = useState<PlatformRole>("member");
  const [roleEdits, setRoleEdits] = useState<Record<string, MembershipRole>>({});
  const [saveError, setSaveError] = useState("");
  const [saveNotice, setSaveNotice] = useState("");
  const [conflict, setConflict] = useState(false);
  const [partial, setPartial] = useState<{ done: string; error?: string } | null>(null);
  const [unknown, setUnknown] = useState<{ requestKey: string; scope: "profile" | "memberships"; error?: string; info?: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [reloadCounter, setReloadCounter] = useState(0);
  const patchKeyRef = useRef<{ stamp: string; key: string } | null>(null);
  const putKeyRef = useRef<{ stamp: string; key: string } | null>(null);
  const busyRef = useRef(false);
  // 对话框卸载或数据代次更新后，迟到的响应不再写入
  const aliveRef = useRef(true);
  const generationRef = useRef(0);
  useEffect(() => () => { aliveRef.current = false; }, []);

  useEffect(() => {
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    const fresh = () => aliveRef.current && generationRef.current === generation;
    const controller = new AbortController();
    setDetail(null);
    setMemberships(null);
    setLoadError("");
    setMembershipsError("");
    setSaveError("");
    setSaveNotice("");
    setConflict(false);
    setPartial(null);
    setUnknown(null);
    patchKeyRef.current = null;
    putKeyRef.current = null;
    void accountsApi.account(apiKey, principalId, controller.signal).then(value => {
      if (!fresh()) return;
      setDetail(value);
      setName(value.display_name);
      setRole(value.platform_role);
    }).catch(error => {
      if (!fresh()) return;
      setLoadError(`${accountError(error)}。`);
    });
    void accountsApi.getMemberships(apiKey, principalId, controller.signal).then(value => {
      if (!fresh()) return;
      setMemberships({ generation: value.generation, items: value.memberships });
      setRoleEdits({});
    }).catch(error => {
      if (!fresh()) return;
      setMembershipsError(`${accountError(error)}。`);
    });
    return () => controller.abort();
  }, [apiKey, principalId, refreshSignal, reloadCounter]);

  const stableRefKey = (ref: typeof patchKeyRef, stamp: string): string => {
    const existing = ref.current;
    if (existing && existing.stamp === stamp) return existing.key;
    const key = newRequestKey();
    ref.current = { stamp, key };
    return key;
  };

  const sharedMemberships: WorkspaceRef[] = memberships?.items.filter(item => !item.personal) ?? [];
  const personalMembership = memberships?.items.find(item => item.personal) ?? null;
  const membershipsDirty = sharedMemberships.some(item => (roleEdits[item.workspace_id] ?? item.role) !== item.role);

  const submit = async () => {
    if (!detail || busyRef.current) return;
    const generation = generationRef.current;
    const fresh = () => aliveRef.current && generationRef.current === generation;
    const trimmed = name.trim();
    if (!trimmed) { setSaveError("请输入显示名称。"); return; }
    setSaveError(""); setSaveNotice(""); setConflict(false);
    busyRef.current = true;
    setBusy(true);
    try {
      let profileSaved = false;
      const patchDirty = trimmed !== detail.display_name || role !== detail.platform_role;
      if (patchDirty) {
        const body = {
          display_name: trimmed !== detail.display_name ? trimmed : undefined,
          platform_role: role !== detail.platform_role ? role : undefined,
          expected_revision: detail.revision,
        };
        const key = stableRefKey(patchKeyRef, `patch:${principalId}:${trimmed}:${role}:${detail.revision}`);
        try {
          const result = await accountsApi.update(apiKey, principalId, body, key);
          if (!fresh()) return;
          patchKeyRef.current = null;
          setDetail(result.account);
          onSaved(result.account);
          profileSaved = true;
          // 自己的角色/状态变更会递增授权代次：让 App 先重读 /v2/me 再核对资料。
          if (selfEdit) onProfileChange();
        } catch (error) {
          if (!fresh()) return;
          patchKeyRef.current = null;
          if (isAccountOutcomeUnknown(error)) {
            setUnknown({ requestKey: key, scope: "profile" });
            return;
          }
          if (error instanceof RequestError && error.status === 409 && error.code === "revision_conflict") {
            setConflict(true);
          } else {
            setSaveError(`${definiteText(error, true)}。`);
          }
          return;
        }
      }
      if (membershipsDirty && memberships) {
        const payload = sharedMemberships.map(item => ({
          workspace_id: item.workspace_id,
          role: roleEdits[item.workspace_id] ?? item.role,
          approve: item.approve,
        }));
        const expectedGeneration = memberships.generation;
        const key = stableRefKey(putKeyRef, `put:${principalId}:${expectedGeneration}:${JSON.stringify(payload)}`);
        try {
          const result = await accountsApi.memberships(apiKey, principalId, {
            memberships: payload,
            expected_generation: expectedGeneration,
          }, key);
          if (!fresh()) return;
          putKeyRef.current = null;
          // MembershipsSet 只含 {workspace_id, role, approve}：沿用已读取的显示名与 personal 标记
          setMemberships(current => current ? {
            generation: result.generation,
            items: result.memberships.map(item => {
              const known = current.items.find(knownItem => knownItem.workspace_id === item.workspace_id);
              return {
                workspace_id: item.workspace_id,
                role: item.role,
                approve: item.approve,
                display_name: known?.display_name ?? item.workspace_id,
                personal: known?.personal ?? false,
              };
            }),
          } : current);
          setRoleEdits({});
          showToast("工作区权限已更新。");
          onClose();
          return;
        } catch (error) {
          if (!fresh()) return;
          putKeyRef.current = null;
          if (isAccountOutcomeUnknown(error)) {
            setPartial({ done: profileSaved ? "账号资料已保存。" : "", error: undefined });
            setUnknown({ requestKey: key, scope: "memberships" });
            return;
          }
          if (error instanceof RequestError && error.status === 409 && error.code === "revision_conflict") {
            setPartial({
              done: profileSaved ? "账号资料已保存。" : "",
              error: "权限版本已在其他页面更新，请重新读取后再保存。",
            });
          } else {
            setPartial({
              done: profileSaved ? "账号资料已保存。" : "",
              error: `工作区权限保存未完成：${definiteText(error)}。`,
            });
          }
          return;
        }
      }
      if (patchDirty) {
        showToast("账号资料已更新。");
        onClose();
      }
    } finally {
      if (aliveRef.current) {
        busyRef.current = false;
        setBusy(false);
      }
    }
  };

  // 未知结果只查不改：按原 request_key 查管理记录；确未执行时由用户重新保存（新请求标识）。
  const queryEditUnknown = async () => {
    const current = unknown;
    if (!current || busyRef.current) return;
    const generation = generationRef.current;
    const fresh = () => aliveRef.current && generationRef.current === generation;
    busyRef.current = true;
    try {
      const page = await accountsApi.events(apiKey, { offset: 0, limit: 5, request_key: current.requestKey });
      if (!fresh()) return;
      if (page.items.length === 0) {
        setUnknown(state => state
          ? {
            ...state,
            info: "没有找到该请求的完成记录，原操作可能未执行。可关闭提示后重新保存，将使用新的请求标识。",
            error: undefined,
          }
          : state);
      } else if (current.scope === "memberships") {
        setUnknown(null);
        const view = await accountsApi.getMemberships(apiKey, principalId);
        if (!fresh()) return;
        setMemberships({ generation: view.generation, items: view.memberships });
        setRoleEdits({});
        setPartial(null);
        showToast("工作区权限已更新。");
      } else {
        setUnknown(null);
        const view = await accountsApi.account(apiKey, principalId);
        if (!fresh()) return;
        setDetail(view);
        setName(view.display_name);
        setRole(view.platform_role);
        setSaveNotice("账号资料已保存，已读取最新内容。");
        if (selfEdit) onProfileChange();
      }
    } catch (error) {
      if (!fresh()) return;
      setUnknown(state => state ? { ...state, info: undefined, error: `${accountError(error)}。` } : state);
    } finally {
      if (aliveRef.current) busyRef.current = false;
    }
  };

  const reloadAll = () => {
    setRoleEdits({});
    setReloadCounter(value => value + 1);
  };
  const reloadMemberships = async () => {
    const generation = generationRef.current;
    try {
      const view = await accountsApi.getMemberships(apiKey, principalId);
      if (!aliveRef.current || generationRef.current !== generation) return;
      setMemberships({ generation: view.generation, items: view.memberships });
      setRoleEdits({});
      setMembershipsError("");
      setPartial(null);
    } catch (error) {
      if (!aliveRef.current || generationRef.current !== generation) return;
      setPartial(state => state ? { ...state, error: `${accountError(error)}。` } : state);
    }
  };

  const target: ConfirmTarget | null = detail
    ? {
      principal_id: detail.principal_id,
      display_name: detail.display_name,
      platform_role: detail.platform_role,
      status: detail.status,
      revision: detail.revision,
      has_active_key: detail.has_active_key,
      key: detail.key,
    }
    : null;

  return (
    <SettingsDialog
      open
      wide
      onClose={onClose}
      title="账号详情"
      footer={<>
        <Button variant="outline" type="button" onClick={onClose}>关闭</Button>
        <Button type="button" disabled={busy || detail === null} onClick={() => void submit()}>保存更改</Button>
      </>}
    >
      {loadError ? (
        <div className="account-empty" role="alert">
          <TriangleAlert aria-hidden="true" />
          <h3>无法读取账号详情</h3>
          <p>{loadError}</p>
          <Button variant="outline" type="button" onClick={reloadAll}>重新读取</Button>
        </div>
      ) : detail === null ? (
        <div aria-busy="true" aria-label="正在读取账号详情">
          <div className="account-skeleton-row"><span className="account-skeleton wide" /><span className="account-skeleton" /></div>
        </div>
      ) : (
        <>
          <div className="account-field">
            <Label htmlFor="account-edit-name">显示名称</Label>
            <Input
              id="account-edit-name"
              className="account-input"
              value={name}
              maxLength={64}
              autoComplete="off"
              onChange={event => setName(event.target.value)}
              onKeyDown={event => { if (event.key === "Enter") { event.preventDefault(); void submit(); } }}
            />
          </div>
          <div className="account-field">
            <Label htmlFor="account-edit-role">平台角色</Label>
            <SelectWrap
              label="平台角色"
              value={role}
              onChange={value => setRole(value as PlatformRole)}
              options={PLATFORM_ROLE_OPTIONS}
            />
            <p className="account-hint">管理员可以管理账号，不会自动获得他人文件的访问权限。</p>
          </div>
          {saveError ? <p className="account-inline-error" role="alert">{saveError}</p> : null}
          {conflict && (
            <div className="account-notice account-notice-warning account-notice-row" role="alert">
              <span>账号已在其他页面更新。重新读取后再保存。</span>
              <Button variant="outline" type="button" onClick={reloadAll}>重新读取</Button>
            </div>
          )}
          {saveNotice ? <p className="account-hint" role="status">{saveNotice}</p> : null}
          {unknown && (
            <div className="account-notice" role="alert">
              <Clock aria-hidden="true" />{unknown.scope === "profile" ? "账号资料保存的结果尚未确认。" : "工作区权限保存的结果尚未确认。"}
              <div className="account-unknown-actions">
                <Button variant="outline" type="button" disabled={busy} onClick={() => void queryEditUnknown()}>查询原操作结果</Button>
              </div>
              <p className="account-hint">请求标识 <span className="account-mono">{unknown.requestKey}</span></p>
              {unknown.info ? <p className="account-hint">{unknown.info}</p> : null}
              {unknown.error ? <p className="account-inline-error">{unknown.error}</p> : null}
            </div>
          )}
          {partial && !unknown && (
            <div className="account-notice account-notice-warning" role="alert">
              {partial.done ? <p>{partial.done}</p> : null}
              {partial.error ? <p className="account-inline-error">{partial.error}</p> : null}
              <div className="account-unknown-actions">
                <Button variant="ghost" type="button" disabled={busy} onClick={() => void reloadMemberships()}>重新读取权限</Button>
              </div>
            </div>
          )}

          <div className="account-modal-divider" />
          <div className="account-section-title"><h3>账号状态</h3><StatusBadge status={detail.status} /></div>
          <div className="account-section-title account-section-title-gap"><h3>访问密钥</h3><span className="account-muted">{keyState(detail)}</span></div>
          <p className="account-small-paragraph account-mono">{detail.key?.masked ?? "尚未签发访问密钥"}</p>
          {target && (
            <div className="account-key-actions">
              <Button variant="outline" type="button" onClick={() => onIssue(target)}>{detail.key?.status === "active" ? "重新签发密钥" : "签发密钥"}</Button>
              {detail.key?.status === "active" && <Button variant="ghost" className="account-danger-action" type="button" onClick={() => onRevoke(target)}>吊销</Button>}
            </div>
          )}

          <div className="account-modal-divider" />
          <div className="account-section-title"><h3>工作区权限</h3></div>
          {memberships === null ? (
            <p className="account-hint" role="status">{membershipsError || "正在读取工作区权限。"}</p>
          ) : (
            <>
              {personalMembership && <div className="account-workspace-permission">
                <span>{workspaceLabel(personalMembership)}<small>账号自有工作区，保留管理权限</small></span>
                <span className="account-muted">管理员</span>
              </div>}
              {memberships.items.length === 0 && <p className="account-hint">尚未分配工作区权限。</p>}
              {sharedMemberships.map(item => (
                <div className="account-workspace-permission" key={item.workspace_id}>
                  <span title={item.display_name}>{workspaceLabel(item)}</span>
                  <SelectWrap
                    label={`${workspaceLabel(item)}的角色`}
                    value={roleEdits[item.workspace_id] ?? item.role}
                    onChange={value => setRoleEdits(current => ({ ...current, [item.workspace_id]: value as MembershipRole }))}
                    options={WORKSPACE_ROLE_OPTIONS}
                  />
                </div>
              ))}
            </>
          )}
          <details className="account-metadata">
            <summary>账号信息</summary>
            <dl className="account-detail-list">
              <div className="account-detail-row"><dt>账号 ID</dt><dd className="account-mono">{detail.principal_id}</dd></div>
            </dl>
          </details>
        </>
      )}
    </SettingsDialog>
  );
}
