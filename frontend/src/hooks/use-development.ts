import { useEffect, useRef, useState } from "react";
import { request, RequestError } from "../api.ts";
import {
  findPendingWrite, parseBinding, parseEnvironment, parseDirectory, parseFileData, parseMcpConnections,
  parseOperation, parseProject, parseProjectsPage, parseSecretResult, parseToolCatalog,
  readDevWrites, saveDevWrites, type DevWriteIntent, type EnvironmentView,
  type McpConnection, type SandboxProject, type SessionBinding, type ToolEntry,
} from "../lib/development-contracts.ts";
import type { SessionState } from "./use-session.ts";

// 开发信息栏数据面：只读取后端真实存在的接口；身份、会话或 fence 任一变化即作废在途读取。

const TRANSITIONAL = ["creating", "starting", "stopping"];
const POLL_MS = 3000;
const MAX_FILE_CHUNKS = 8;
const CHUNK = 65536;
const ENVIRONMENT_PATH = (projectId: string) => `/v2/me/development/projects/${encodeURIComponent(projectId)}/environment`;
// These binding errors arise before the transactional mutation commits. Other errors,
// including storage/transport failures and operation_unknown, retain the original intent.
function bindingRejected(error: unknown): boolean {
  return error instanceof RequestError && error.status >= 400 && error.status < 500
    && ["revision_conflict", "permission_denied", "not_found", "invalid_input", "workspace_busy", "session_closed", "workspace_contract_required", "unauthorized"].includes(error.code);
}

export async function fetchProjectsPage(apiKey: string, signal?: AbortSignal) {
  return parseProjectsPage(await request("/v2/me/sandbox/projects?offset=0&limit=100", apiKey, undefined, signal));
}
export async function fetchDirectory(apiKey: string, projectId: string, path: string, signal?: AbortSignal, offset = 0) {
  return parseDirectory(await request(
    `/v2/me/sandbox/projects/${encodeURIComponent(projectId)}/files?path=${encodeURIComponent(path)}&limit=100${offset ? `&offset=${offset}` : ""}`,
    apiKey, undefined, signal));
}
// 顺序拼接最多 MAX_FILE_CHUNKS 块；逐块核对 stat_token 与 size，文件在读取期间变化即失败。
// 返回的 sha256 是后端对整个文件计算的摘要，不因分段而失真。
export async function fetchFileBytes(apiKey: string, projectId: string, path: string, signal?: AbortSignal) {
  let offset = 0, sha256 = "", size = 0, eof = false, mayChange = false;
  let statToken: string | null = null;
  const parts: Uint8Array[] = [];
  for (let index = 0; index < MAX_FILE_CHUNKS; index++) {
    const raw = parseFileData(await request(
      `/v2/me/sandbox/projects/${encodeURIComponent(projectId)}/file?path=${encodeURIComponent(path)}&offset=${offset}&limit=${CHUNK}`,
      apiKey, undefined, signal));
    if (raw.path !== path) throw new Error("文件身份不匹配");
    if (raw.offset !== offset) throw new Error("文件响应不连续");
    const token = JSON.stringify(raw.stat_token);
    if (statToken === null) statToken = token;
    else if (statToken !== token || raw.size !== size || raw.sha256 !== sha256) throw new Error("文件在读取期间发生变化");
    sha256 = raw.sha256; size = raw.size; eof = raw.eof; mayChange = mayChange || raw.may_change;
    const binary = atob(raw.data);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    parts.push(bytes);
    offset += bytes.length;
    if (raw.eof) break;
  }
  const total = parts.reduce((sum, part) => sum + part.length, 0);
  const bytes = new Uint8Array(total);
  let cursor = 0;
  for (const part of parts) { bytes.set(part, cursor); cursor += part.length; }
  if (eof && globalThis.crypto?.subtle) {
    const digest = [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))].map(value => value.toString(16).padStart(2, "0")).join("");
    if (digest !== sha256) throw new Error("文件内容校验失败，请重新读取");
  }
  return { bytes, sha256, size, eof, may_change: mayChange };
}

function noticeFor(error: unknown): string {
  if (error instanceof RequestError) {
    const reasons: Record<string, string> = {
      workspace_busy: "对话或项目正在运行，结束后再试",
      sandbox_management_unavailable: "开发管理服务未启用",
      sandbox_backend_unavailable: "沙箱服务未连接",
      preview_unavailable: "预览服务未启用",
      storage_unavailable: "暂时无法保存",
      revision_conflict: "状态已更新，本次未执行；请刷新后重试",
      not_found: "未找到记录，请检查项目与身份",
      permission_denied: "当前身份无权执行此操作",
      unauthorized: "密钥无效，请重新连接",
      schema_upgrade_required: "账号服务需要升级，暂不可用",
    };
    return reasons[error.code] ?? "读取失败，请刷新";
  }
  return error instanceof Error && ["绑定身份不匹配", "项目身份不匹配", "环境身份不匹配", "文件身份不匹配", "文件响应不连续", "文件在读取期间发生变化", "文件内容校验失败，请重新读取", "未确认请求过多", "无法保存请求", "无法读取未确认请求"].includes(error.message)
    ? error.message : "读取失败，请刷新";
}

export function useDevelopment(chat: SessionState, apiKey: string, active: boolean, fence: { current: number }) {
  const [binding, setBinding] = useState<SessionBinding | null>(null);
  const [project, setProject] = useState<SandboxProject | null>(null);
  const [environment, setEnvironment] = useState<EnvironmentView | null>(null);
  const [tools, setTools] = useState<ToolEntry[]>([]);
  const [connections, setConnections] = useState<McpConnection[] | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [writes, setWrites] = useState<DevWriteIntent[]>([]);
  const [secretRefs, setSecretRefs] = useState<{ name: string; revision: number }[]>([]);
  const [version, setVersion] = useState(0);
  const [flightCount, setFlightCount] = useState(0);
  const mounted = useRef(false), epoch = useRef(0), flights = useRef(new Set<string>());
  const bindingRef = useRef<SessionBinding | null>(null); bindingRef.current = binding;
  // Secret 版本仅存内存（来自本会话写回执），按 主体+项目+引用名 划分作用域；刷新后清零。
  const secretRevisions = useRef(new Map<string, number>());
  const secretScope = (projectId: string, name: string) => {
    const actor = chatRef.current.actor;
    return `${actor?.principal_id ?? ""}:${actor?.generation ?? 0}:${projectId}:${name}`;
  };
  const identity = JSON.stringify([
    chat.actor?.principal_id ?? null, chat.actor?.generation ?? null, chat.workspace, chat.id,
    chat.session?.target_key ?? null, apiKey, active, fence.current,
  ]);
  const identityRef = useRef(identity); identityRef.current = identity;
  const chatRef = useRef(chat); chatRef.current = chat;
  const keyRef = useRef(apiKey); keyRef.current = apiKey;
  function capture() {
    const currentEpoch = epoch.current, currentIdentity = identityRef.current, key = apiKey, fenceValue = fence.current;
    return () => mounted.current && active && epoch.current === currentEpoch && identityRef.current === currentIdentity
      && keyRef.current === key && fence.current === fenceValue;
  }
  function flight(marker: string, value: boolean) {
    if (value) flights.current.add(marker); else flights.current.delete(marker);
    if (mounted.current) setFlightCount(flights.current.size);
  }
  function persistWrites(values: DevWriteIntent[]) {
    saveDevWrites(localStorage, values);
    if (mounted.current) setWrites(values);
  }
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; epoch.current++; };
  }, []);
  useEffect(() => {
    epoch.current++;
    const controller = new AbortController();
    setBinding(null); setProject(null); setEnvironment(null); setTools([]); setConnections(null); setNotice(null); setSecretRefs([]);
    secretRevisions.current.clear();
    const current = capture();
    // 鉴权以 chat.ready/actor 为准（legacy/local 可用空密钥），不以字符串非空为准。
    if (!active || !chat.actor || !chat.id) return () => controller.abort();
    const reloadWrites = () => {
      try {
        return readDevWrites(localStorage).filter(item =>
          item.principal_id === chat.actor!.principal_id && item.generation === chat.actor!.generation
          && item.workspace_id === chat.workspace && item.session_id === chat.id);
      } catch { setNotice("无法读取未确认请求"); return []; }
    };
    setWrites(reloadWrites());
    // 绑定/项目/环境、工具目录、账号连接三条链路独立加载；任一失败不影响其余。
    async function loadProject() {
      const rawBinding = parseBinding(await request(
        `/v2/me/sandbox/sessions/${encodeURIComponent(chat.id)}/project`, apiKey, undefined, controller.signal));
      if (!current()) return;
      if (rawBinding.session_id !== chat.id || rawBinding.workspace_id !== chat.workspace) throw new Error("绑定身份不匹配");
      setBinding(rawBinding);
      if (!rawBinding.project_id) return;
      const rawProject = parseProject(await request(
        `/v2/me/sandbox/projects/${encodeURIComponent(rawBinding.project_id)}`, apiKey, undefined, controller.signal));
      if (!current()) return;
      if (rawProject.project_id !== rawBinding.project_id) throw new Error("项目身份不匹配");
      setProject(rawProject);
      try {
        const rawEnvironment = parseEnvironment(await request(ENVIRONMENT_PATH(rawBinding.project_id), apiKey, undefined, controller.signal));
        if (!current()) return;
        if (rawEnvironment.project_id !== rawBinding.project_id) throw new Error("环境身份不匹配");
        setEnvironment(rawEnvironment);
      } catch (error) {
        if (!current()) return;
        // 404 表示该项目尚未准备开发环境；保持 null，界面显示"未准备"。
        if (!(error instanceof RequestError && error.status === 404)) throw error;
      }
    }
    async function loadTools() {
      const target = chat.session?.target_key;
      if (!target) return;
      const rawTools = await request(`/v2/targets/${encodeURIComponent(target)}/tools`, apiKey, undefined, controller.signal);
      if (!current()) return;
      setTools(parseToolCatalog(rawTools));
    }
    async function loadConnections() {
      const rawConnections = await request("/v2/me/mcp-connections", apiKey, undefined, controller.signal);
      if (!current()) return;
      setConnections(parseMcpConnections(rawConnections));
    }
    for (const load of [loadProject, loadTools, loadConnections]) {
      void load().catch(error => {
        if (!current() || controller.signal.aborted) return;
        if (load === loadConnections && error instanceof RequestError && [401, 403, 404, 409, 503].includes(error.status)) { setConnections(null); return; }
        setNotice(noticeFor(error));
      });
    }
    return () => controller.abort();
  }, [identity, version]);
  // Preparing can begin long after initial load; start polling whenever it enters a transition.
  useEffect(() => {
    if (!active || !environment || !TRANSITIONAL.includes(environment.state)) return;
    const controller = new AbortController(), current = capture(), projectId = environment.project_id;
    let timer: ReturnType<typeof setTimeout>;
    async function inspect() {
      try {
        const fresh = parseEnvironment(await request(ENVIRONMENT_PATH(projectId), apiKey, undefined, controller.signal));
        if (!current() || controller.signal.aborted) return;
        if (fresh.project_id !== projectId) throw new Error("环境身份不匹配");
        setEnvironment(fresh);
        if (TRANSITIONAL.includes(fresh.state)) timer = setTimeout(inspect, POLL_MS);
      } catch (error) { if (current() && !controller.signal.aborted) setNotice(noticeFor(error)); }
    }
    timer = setTimeout(inspect, POLL_MS);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [identity, version, active, environment?.project_id, environment?.state]);
  function reload() { epoch.current++; setVersion(value => value + 1); }
  function sameScope(intent: DevWriteIntent) {
    const value = chatRef.current;
    return !!value.ready && !!value.actor
      && intent.principal_id === value.actor.principal_id
      && intent.generation === value.actor.generation
      && intent.workspace_id === value.workspace && intent.session_id === value.id;
  }
  // 绑定项目：已有同 scope 未确认记录时先查回执而非用新键重复提交；提交前账本先行。
  async function bindProject(projectId: string | null) {
    const value = chatRef.current;
    if (!value.ready || !value.actor || !value.id || !value.session) return;
    if (!value.canWrite || value.session.status !== "open") { setNotice("当前身份无权执行此操作"); return; }
    const marker = "bind:" + value.actor.principal_id + ":" + value.id;
    if (flights.current.has(marker)) return;
    try {
      const pending = findPendingWrite(readDevWrites(localStorage), "bind-project", {
        principal_id: value.actor.principal_id, generation: value.actor.generation,
        workspace_id: value.workspace, session_id: value.id,
      });
      if (pending) { void recoverPending(pending); setNotice("上一次绑定结果未确认，正在查找"); return; }
    } catch (error) { setNotice(noticeFor(error)); return; }
    const current = capture();
    let attemptedKey: string | null = null;
    flight(marker, true);
    try {
      const fresh = parseBinding(await request(
        `/v2/me/sandbox/sessions/${encodeURIComponent(value.id)}/project`, apiKey));
      if (!current()) return;
      if (fresh.session_id !== value.id || fresh.workspace_id !== value.workspace) throw new Error("绑定身份不匹配");
      if (fresh.project_id === projectId) { setBinding(fresh); return; }
      const requestKey = crypto.randomUUID();
      const target = `/v2/me/sandbox/sessions/${encodeURIComponent(value.id)}/project`;
      const body = {
        request_key: requestKey,
        expected_revision: fresh.revision,
        expected_session_revision: fresh.session_revision,
        project_id: projectId,
      };
      const ledger = readDevWrites(localStorage).filter(item => item.request_key !== requestKey);
      ledger.push({
        principal_id: value.actor.principal_id, generation: value.actor.generation,
        workspace_id: value.workspace, session_id: value.id, project_id: projectId,
        kind: "bind-project", request_key: requestKey, target, body, rejected: false,
      });
      persistWrites(ledger);
      attemptedKey = requestKey;
      const raw = parseBinding(await request(target, apiKey, body, undefined, "PUT"));
      if (!current()) return;
      if (raw.session_id !== value.id || raw.workspace_id !== value.workspace) throw new Error("绑定身份不匹配");
      persistWrites(readDevWrites(localStorage).filter(item => item.request_key !== requestKey));
      setBinding(raw);
      setProject(null); setEnvironment(null);
      reload();
      value.refresh();
    } catch (error) {
      if (!current()) return;
      if (attemptedKey && bindingRejected(error)) {
        try { persistWrites(readDevWrites(localStorage).filter(item => item.request_key !== attemptedKey)); }
        catch { setNotice("无法清理已拒绝请求，请检查本机存储后重新读取"); return; }
      }
      if (error instanceof TypeError) setNotice("绑定结果未知；已保留请求记录，可在信息栏查找结果");
      else setNotice(noticeFor(error));
    } finally { flight(marker, false); }
  }
  // 准备环境：已有同 scope 未确认记录时先查回执；request_key 先入账本。
  async function prepareEnvironment() {
    const value = chatRef.current;
    const projectId = binding?.project_id;
    if (!value.ready || !value.actor || !projectId) return;
    if (!value.canWrite) { setNotice("当前身份无权执行此操作"); return; }
    const marker = "prepare:" + value.actor.principal_id + ":" + projectId;
    if (flights.current.has(marker)) return;
    try {
      const pending = findPendingWrite(readDevWrites(localStorage), "prepare-environment", {
        principal_id: value.actor.principal_id, generation: value.actor.generation,
        workspace_id: value.workspace, session_id: value.id, project_id: projectId,
      });
      if (pending) { void recoverPending(pending); setNotice("上一次准备结果未确认，正在查找"); return; }
    } catch (error) { setNotice(noticeFor(error)); return; }
    const current = capture();
    flight(marker, true);
    try {
      const requestKey = crypto.randomUUID();
      const target = `/v2/me/development/projects/${encodeURIComponent(projectId)}/prepare`;
      const body = { request_key: requestKey };
      const ledger = readDevWrites(localStorage).filter(item => item.request_key !== requestKey);
      ledger.push({
        principal_id: value.actor.principal_id, generation: value.actor.generation,
        workspace_id: value.workspace, session_id: value.id, project_id: projectId,
        kind: "prepare-environment", request_key: requestKey, target, body, rejected: false,
      });
      persistWrites(ledger);
      const raw = parseEnvironment(await request(target, apiKey, body));
      if (!current()) return;
      if (raw.project_id !== projectId) throw new Error("环境身份不匹配");
      persistWrites(readDevWrites(localStorage).filter(item => item.request_key !== requestKey));
      setEnvironment(raw);
    } catch (error) {
      if (!current()) return;
      if (error instanceof TypeError) setNotice("准备结果未知；已保留请求记录，可在信息栏查找结果");
      else setNotice(noticeFor(error));
    } finally { flight(marker, false); }
  }
  async function stopEnvironment() {
    const value = chatRef.current;
    const projectId = binding?.project_id;
    if (!value.ready || !value.actor || !projectId) return;
    if (!value.canWrite) { setNotice("当前身份无权执行此操作"); return; }
    const marker = "stop:" + value.actor.principal_id + ":" + projectId;
    if (flights.current.has(marker)) return;
    const current = capture();
    flight(marker, true);
    try {
      const raw = parseEnvironment(await request(
        `/v2/me/development/projects/${encodeURIComponent(projectId)}/stop`, apiKey, undefined, undefined, "POST"));
      if (!current()) return;
      if (raw.project_id !== projectId) throw new Error("环境身份不匹配");
      setEnvironment(raw);
    } catch (error) {
      if (!current()) return;
      if (error instanceof TypeError) setNotice("停止结果未知；可稍后查看环境状态确认");
      else setNotice(noticeFor(error));
    } finally { flight(marker, false); }
  }
  // Secret 只写不读：值仅在请求体中出现一次，成功后只保留版本号；无法核实时不重试。
  async function writeSecret(input: { secretId: string; value: string; hours: number; mode: "add" | "rotate" | "revoke" }): Promise<string> {
    const value = chatRef.current;
    const projectId = binding?.project_id;
    if (!value.ready || !value.actor || !projectId) throw new Error("请先连接并绑定项目");
    if (!value.canApprove) throw new Error("当前身份无权执行此操作");
    const trimmed = input.secretId.trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(trimmed)) throw new Error("引用名称需以字母或数字开头，仅含字母、数字、点、下划线、连字符");
    if (!Number.isFinite(input.hours) || input.hours <= 0 || input.hours > 24) throw new Error("有效期需在 1-24 小时之间");
    if (input.value.length === 0 || new TextEncoder().encode(input.value).length > 8192) throw new Error("凭据值长度无效");
    const marker = "secret:" + value.actor.principal_id + ":" + projectId + ":" + trimmed;
    if (flights.current.has(marker)) throw new Error("同一引用正在提交");
    const current = capture();
    flight(marker, true);
    try {
      const scopeKey = secretScope(projectId, trimmed);
      const known = secretRevisions.current.get(scopeKey);
      if (known === undefined && input.mode !== "add") throw new Error("该引用的版本未知；请重新读取后检查");
      if (known !== undefined && input.mode === "add") throw new Error("该引用已在本会话写入，请选择轮换");
      const body = {
        expected_revision: known ?? 0,
        expires_at: Math.floor(Date.now() / 1000) + Math.round(input.hours * 3600) - 60,
        value: input.value,
        revoked: input.mode === "revoke",
      };
      const raw = parseSecretResult(await request(
        `/v2/me/development/projects/${encodeURIComponent(projectId)}/secrets/${encodeURIComponent(trimmed)}`,
        apiKey, body, undefined, "PUT"));
      // 迟到回执或项目已变化：不更新任何作用域状态。
      if (!current() || bindingRef.current?.project_id !== projectId) {
        throw new Error("范围已变化，本次结果未确认；请勿重复使用同一引用名称");
      }
      if (raw.secret_id !== trimmed || raw.revoked !== (input.mode === "revoke")) throw new Error("凭据回执不匹配");
      secretRevisions.current.set(scopeKey, raw.revision);
      setSecretRefs(currentRefs => [...currentRefs.filter(item => item.name !== trimmed), { name: trimmed, revision: raw.revision }]);
      if (input.mode === "revoke") return `引用已撤销（版本 ${raw.revision}）`;
      return `引用已保存（版本 ${raw.revision}）`;
    } catch (error) {
      if (error instanceof RequestError && error.status === 409 && error.code === "revision_conflict") {
        throw new Error("该引用已存在且版本未知；面板不读取凭据，为避免覆盖请改用新的引用名称");
      }
      if (error instanceof TypeError) throw new Error("写入结果未知；请勿重复提交同一引用，如需再写请更换引用名称");
      if (error instanceof RequestError) throw new Error(noticeFor(error));
      throw error instanceof Error ? error : new Error("写入未完成");
    } finally { flight(marker, false); }
  }
  // 回执核实：bind 以 sandbox operations 为准（404 证明未执行才原键重投）；
  // prepare 以 development 环境记录为准，sandbox operations 只用于消息判断，其 404 不作重发依据。
  async function recoverPending(intent: DevWriteIntent) {
    if (!sameScope(intent) || intent.rejected) return;
    const marker = "recover:" + intent.request_key;
    if (flights.current.has(marker)) return;
    let replayedBinding = false;
    const current = capture();
    flight(marker, true);
    try {
      const ledger = () => readDevWrites(localStorage);
      if (intent.kind === "prepare-environment") {
        let materialized: EnvironmentView | null = null;
        try {
          materialized = parseEnvironment(await request(ENVIRONMENT_PATH(intent.project_id!), apiKey));
        } catch (error) {
          if (!(error instanceof RequestError && error.status === 404)) throw error;
        }
        if (!current()) return;
        if (materialized && materialized.project_id === intent.project_id) {
          setEnvironment(materialized);
          if (materialized.state === "ready") {
            persistWrites(ledger().filter(item => item.request_key !== intent.request_key));
            setNotice("环境已就绪");
          } else if (["creating", "starting", "stopping"].includes(materialized.state)) {
            setNotice("环境状态仍在变化，请稍后重新读取"); reload();
          } else setNotice("环境尚未就绪，请检查环境状态；本次准备结果仍未确认");
          return;
        }
        let receipt: string | null = null;
        try {
          receipt = parseOperation(await request(
            `/v2/me/sandbox/operations?request_key=${encodeURIComponent(intent.request_key)}`, apiKey)).status;
        } catch (error) {
          if (!(error instanceof RequestError && error.status === 404)) throw error;
        }
        if (!current()) return;
        if (receipt === "pending") { setNotice("之前的准备仍在处理中"); return; }
        if (receipt === "failed") {
          persistWrites(ledger().filter(item => item.request_key !== intent.request_key));
          setNotice("之前的准备未生效，可重新操作");
          return;
        }
        // 环境记录确无 + provisioning 未挂起：按原键重投，后端按 request_key 去重。
        const raw = parseEnvironment(await request(intent.target, apiKey, intent.body));
        if (!current()) return;
        if (raw.project_id !== intent.project_id) throw new Error("环境身份不匹配");
        persistWrites(ledger().filter(item => item.request_key !== intent.request_key));
        setEnvironment(raw); setNotice("已按原请求找回并生效");
        return;
      }
      let receipt: string | null = null;
      try {
        receipt = parseOperation(await request(
          `/v2/me/sandbox/operations?request_key=${encodeURIComponent(intent.request_key)}`, apiKey)).status;
      } catch (error) {
        if (!(error instanceof RequestError && error.status === 404)) throw error;
      }
      if (!current()) return;
      if (receipt === "completed") {
        persistWrites(ledger().filter(item => item.request_key !== intent.request_key));
        setNotice("已确认之前的操作生效"); reload(); chatRef.current.refresh();
        return;
      }
      if (receipt === "failed") {
        persistWrites(ledger().filter(item => item.request_key !== intent.request_key));
        setNotice("之前的操作未生效，可重新操作");
        return;
      }
      if (receipt !== null) { setNotice("之前的操作仍在处理中"); return; }
      replayedBinding = true;
      const raw = parseBinding(await request(intent.target, apiKey, intent.body, undefined, "PUT"));
      if (!current()) return;
      if (raw.session_id !== intent.session_id || raw.workspace_id !== intent.workspace_id) throw new Error("绑定身份不匹配");
      persistWrites(ledger().filter(item => item.request_key !== intent.request_key));
      setBinding(raw); setNotice("已按原请求找回并生效"); reload(); chatRef.current.refresh();
    } catch (error) {
      if (!current()) return;
      if (replayedBinding && bindingRejected(error)) {
        try { persistWrites(readDevWrites(localStorage).filter(item => item.request_key !== intent.request_key)); }
        catch { setNotice("无法清理已拒绝请求，请检查本机存储后重新读取"); return; }
        setNotice(noticeFor(error)); return;
      }
      if (error instanceof RequestError && error.status === 401 && error.code === "unauthorized") {
        const ledger = readDevWrites(localStorage);
        const saved = ledger.find(item => item.request_key === intent.request_key);
        if (saved) { saved.rejected = true; persistWrites(ledger); }
        setNotice("密钥无效；请重新连接后查找结果");
        return;
      }
      setNotice(noticeFor(error));
    } finally { flight(marker, false); }
  }
  function discardPending(intent: DevWriteIntent) {
    if (!sameScope(intent)) return;
    try { persistWrites(readDevWrites(localStorage).filter(item => item.request_key !== intent.request_key)); }
    catch (error) { setNotice(noticeFor(error)); }
  }
  const ready = !!chat.ready && chat.id !== "";
  const canWrite = !!chat.canWrite && !!chat.session && chat.session.status === "open";
  const canApprove = !!chat.canApprove && canWrite;
  const actorKey = chat.actor?.principal_id ?? "";
  return {
    binding, project, environment, tools, connections, notice, writes, secretRefs, ready,
    canWrite, canApprove,
    busy: flightCount > 0,
    pendingBind: flights.current.has("bind:" + actorKey + ":" + chat.id),
    preparing: !!binding?.project_id && flights.current.has("prepare:" + actorKey + ":" + binding.project_id),
    stopping: !!binding?.project_id && flights.current.has("stop:" + actorKey + ":" + binding.project_id),
    reload, dismissNotice: () => setNotice(null),
    bindProject, prepareEnvironment, stopEnvironment, writeSecret,
    recoverPending, discardPending,
  };
}
export type DevelopmentState = ReturnType<typeof useDevelopment>;
