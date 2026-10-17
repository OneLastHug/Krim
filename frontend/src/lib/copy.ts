import { RequestError } from "../api";
export const labels: Record<string, string> = { queued: "排队中", running: "执行中", completed: "已完成", failed: "失败", aborted: "已停止", interrupted: "运行中断" };
const reasons: Record<string, string> = { unauthorized: "密钥无效", capacity: "运行名额已满", invalid_input: "任务内容无效", input_limit: "任务内容过长", permission_denied: "没有工具权限", approval_denied: "调用已拒绝", approval_expired: "审批已过期", approval_conflict: "审批决定不一致", approval_unavailable: "审批已结束", tool_timeout: "工具调用超时", run_timeout: "运行超时", storage_unavailable: "暂时无法保存", key_conflict: "请求内容不一致", request_conflict: "原请求内容不一致", invalid_record: "无法读取原请求记录", identity: "响应身份不一致" };
export function reason(error: unknown, fallback = "请求未完成") { const code = codeOf(error); return code ? reasons[code] ?? fallback : fallback; }
export function codeOf(error: unknown) { return error instanceof RequestError ? error.code : error instanceof Error && ["approval_conflict", "request_conflict", "invalid_record", "identity"].includes(error.message) ? error.message : null; }
export function runReason(code: unknown) { return typeof code === "string" ? reasons[code] ?? "执行未完成" : "执行未完成"; }
export const ui = {
    "brand": "Krim Agent",
    "offline": "离线演示",
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
