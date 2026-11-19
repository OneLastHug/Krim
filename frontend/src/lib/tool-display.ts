export type ToolKind = "file" | "terminal" | "search" | "mcp" | "other";
export function toolKind(name: string): ToolKind {
  if (/shell|terminal|exec|command/i.test(name)) return "terminal";
  if (/search|grep|glob|find|browse/i.test(name)) return "search";
  if (/file|read|write|edit|patch|directory/i.test(name)) return "file";
  if (/mcp/i.test(name)) return "mcp";
  return "other";
}
export function toolSummary(value: unknown): string {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "";
  const args = value as Record<string, unknown>;
  const text = args.path ?? args.file_path ?? args.command ?? args.query ?? args.pattern ?? args.url;
  return typeof text === "string" ? text.replace(/\s+/g, " ").slice(0, 180) : "";
}
export function formatToolValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const result = value as Record<string, unknown>;
    if (typeof result.stdout === "string" || typeof result.stderr === "string") return [result.stdout, result.stderr].filter(part => typeof part === "string" && part.length > 0).join("\n");
    if (typeof result.text === "string") return result.text;
    if (typeof result.content === "string") return result.content;
    if (Array.isArray(result.content) && result.content.every(item => item && typeof item === "object" && item.type === "text" && typeof item.text === "string")) return result.content.map(item => item.text).join("\n");
  }
  try { return JSON.stringify(value, null, 2) ?? "未提供"; } catch { return "内容暂时无法显示"; }
}
