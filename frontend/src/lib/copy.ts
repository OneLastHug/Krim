import { RequestError } from "../api";
export const labels: Record<string, string> = { queued: "排队中", running: "执行中", completed: "已完成", failed: "失败", aborted: "已停止", interrupted: "运行中断" };
const reasons: Record<string, string> = { unauthorized: "密钥无效", capacity: "运行名额已满", invalid_input: "任务内容无效", input_limit: "任务内容过长", permission_denied: "没有工具权限", approval_denied: "调用已拒绝", approval_expired: "审批已过期", approval_conflict: "审批决定不一致", approval_unavailable: "审批已结束", tool_timeout: "工具调用超时", run_timeout: "运行超时", storage_unavailable: "暂时无法保存", provider_config: "模型连接配置无效", provider_protocol: "模型响应无效", provider_incomplete_stream: "模型响应未完成", provider_http_error: "模型请求失败", provider_timeout: "模型响应超时", provider_network: "模型连接中断", provider_redirect: "模型地址发生跳转", provider_output_limit: "模型输出达到上限", provider_request_limit: "模型请求过长", provider_response_limit: "模型响应过长", provider_event_limit: "模型响应达到上限", provider_tool_arguments: "工具参数无效", provider_usage_conflict: "模型用量无效", key_conflict: "请求内容不一致", request_conflict: "原请求内容不一致", invalid_record: "无法读取原请求记录", identity: "响应身份不一致" };
export function reason(error: unknown, fallback = "请求未完成") { const code = codeOf(error); return code ? reasons[code] ?? fallback : fallback; }
export function codeOf(error: unknown) { return error instanceof RequestError ? error.code : error instanceof Error && ["approval_conflict", "request_conflict", "invalid_record", "identity"].includes(error.message) ? error.message : null; }
export function runReason(code: unknown) { return typeof code === "string" ? reasons[code] ?? "执行未完成" : "执行未完成"; }
export const ui = {
    "brand": "Krim Agent",
    "offline": "离线演示",
    "agent": "Krim Agent",
    "task": "任务",
    "input": "输入任务",
    "create": "创建任务",
    "find": "查找任务",
    "processing": "处理中…",
    "connect": "连接",
    "openTask": "打开任务",
    "open": "打开",
    "key": "实例密钥",
    "apply": "应用",
    "taskID": "任务 ID",
    "pasteID": "粘贴任务 ID",
    "current": "当前任务",
    "reading": "读取中…",
    "unread": "未能读取",
    "refresh": "刷新状态",
    "stop": "停止",
    "stopping": "停止中…",
    "result": "结果",
    "tools": "工具",
    "trace": "Trace",
    "events": "事件",
    "empty": "暂无任务",
    "pending": "待审批",
    "allow": "允许",
    "deny": "拒绝",
    "editKey": "修改密钥",
    "retry": "重试",
    "taskActions": "任务操作",
    "details": "任务详情",
    "pendingCall": "待审批工具调用",
    "arguments": "参数",
    "raw": "原始记录",
    "errorDetails": "错误详情",
    "noTools": "暂无工具调用",
    "unknownExecution": "执行未知",
    "unknownResult": "结果未知",
    "noResult": "无结果",
    "unknown": "未知",
    "completeRecord": "记录完整",
    "incompleteRecord": "记录不完整",
    "notEnded": "未结束",
    "invalidRecord": "无法读取原请求记录",
    "unknownCreate": "创建结果未确认",
    "readFailed": "读取未完成",
    "unknownStop": "停止结果未确认",
    "unknownApproval": "审批结果未确认"
} as const;
export const evalUi = { entry: "评测", data: "数据", caseId: "样本 ID", input: "输入", expected: "预期结果（JSON）", reason: "理由", status: "预期状态", code: "错误码", constraints: "约束", tools: "工具序列（JSON）", trajectory: "轨迹（JSON）", schema: "结果 Schema（JSON）", source: "来源任务 ID", readSource: "读取来源", useInput: "使用输入", manual: "改为手工样本", add: "加入样本", name: "数据集", version: "版本", freeze: "冻结修订", findRevision: "查找修订", openRevision: "打开修订", target: "目标", repeat: "重复次数", rules: "规则", start: "开始评测", findEval: "查找评测", open: "打开评测", id: "评测 ID", current: "当前评测", reading: "读取中…", unread: "读取未完成", report: "报告", compare: "版本比较", baseline: "基线评测 ID", candidate: "候选评测 ID", compareAction: "比较", empty: "暂无评测", coverage: "覆盖", passed: "通过", usage: "用量", unknown: "未知" } as const;
export const qualityLabels: Record<string, string> = { passed: "通过", failed: "未通过", error: "评分错误", timed_out: "超时", unscored: "未评分" };

export function targetLabel(key: unknown) { return (key === "target-http" ? "HTTP目标" : "脚本") + " · " + String(key); }


export const chatCopy = { tab: "对话", create: "新建对话", createSubmit: "创建对话", title: "对话名称", workspace: "工作区", target: "目标", defaultTarget: "默认", message: "消息", send: "发送", enqueue: "加入队列", empty: "暂无对话", close: "关闭对话", resume: "继续队列", unknown: "提交结果未知", find: "查找原请求", details: "工具与 Trace", history: "对话历史" };
export const turnLabels: Record<string, string> = { queued: "排队中", running: "进行中", completed: "已完成", failed: "失败", cancelled: "已停止", interrupted: "已中断" };
export function chatReason(error: unknown): string {
  const code = codeOf(error) ?? (typeof error === "object" && error !== null && "code" in error && typeof error.code === "string" ? error.code : null);
  const reasons: Record<string, string> = { unauthorized: "密钥无效，请重新连接", capacity: "队列已满，请稍后提交", invalid_input: "请检查输入后提交", input_limit: "输入超出容量，请修改后提交", context_budget: "历史超出上下文容量，请新建对话", provider_http_error: "模型请求失败，可查看详情", provider_timeout: "模型请求超时", queue_timeout: "排队已超时", permission_denied: "当前身份无权执行此操作", not_found: "未找到记录，请检查身份与对话", revision_conflict: "对话已更新，请刷新检查原请求", key_conflict: "原请求内容冲突，请查找原请求", session_closed: "对话已关闭", interrupted: "运行已中断", storage_unavailable: "暂时无法读取记录，请刷新" };
  if (code && reasons[code]) return reasons[code]!;
  const allowed = ["无法保存请求", "无法读取未确认请求", "未确认请求过多", "请先连接", "请连接原身份", "没有可用工作区", "响应身份不匹配", "原请求内容不匹配"];
  return error instanceof Error && allowed.includes(error.message) ? error.message : "读取失败，请刷新";
}

export const workspaceCopy = { title: "工作区任务", unavailable: "工作区未配置", send: "发送工作请求", find: "查找原工作请求" } as const;
