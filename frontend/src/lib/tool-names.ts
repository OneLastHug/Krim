// 人可见工具名迁移：旧 dev_* 名称与迁移后名称共用一套固定已知映射，
// 只用于界面标题、工具目录、审批、Outputs 与历史记录的显示，以及新旧名称的等价比较。
// 事件、持久化 JSON、回传 name 与指纹一律保持原样；本表不参与任何权限或身份判定。

/** 迁移后的规范工具名（确知列表）。 */
export const NEW_TOOL_NAMES = [
  "shell", "process_status", "read_output", "write_stdin", "stop_process",
  "read", "list", "write", "edit", "find", "search",
  "preview_register", "preview_status", "preview_stop",
  "browser_open", "browser_action", "browser_upload", "browser_screenshot", "browser_close",
  "git_clone", "git_diff", "git_tree", "git_commit", "git_push",
] as const;

/** 旧名 → 新名的固定映射。只收录确知条目；表外名称（含外部 dev_* 工具）不剥前缀。 */
export const KNOWN_TOOL_NAMES: Readonly<Record<string, string>> = {
  dev_shell: "shell",
  dev_process: "process_status",
  dev_output: "read_output",
  dev_stdin: "write_stdin",
  dev_cancel: "stop_process",
  dev_read: "read",
  dev_list: "list",
  dev_preview_register: "preview_register",
  dev_preview_status: "preview_status",
  dev_preview_stop: "preview_stop",
  dev_browser_open: "browser_open",
  dev_browser_action: "browser_action",
  dev_browser_upload: "browser_upload",
  dev_browser_screenshot: "browser_screenshot",
  dev_browser_close: "browser_close",
  dev_git_clone: "git_clone",
  dev_git_diff: "git_diff",
  dev_git_tree: "git_tree",
  dev_git_commit: "git_commit",
  dev_git_push: "git_push",
};

// 只认自有键：toString/constructor/__proto__ 等原型名不是工具名，不得读到 Object 原型值。
function mappedName(name: string): string | undefined {
  return Object.hasOwn(KNOWN_TOOL_NAMES, name) ? KNOWN_TOOL_NAMES[name] : undefined;
}

/** 界面显示：已知旧名显示为新名，新名与其余名称原样显示。 */
export function displayToolName(name: string): string {
  return mappedName(name) ?? name;
}

/** 新旧等价比较用规范名：与 displayToolName 使用同一张固定表。 */
export function canonicalToolName(name: string): string {
  return mappedName(name) ?? name;
}
