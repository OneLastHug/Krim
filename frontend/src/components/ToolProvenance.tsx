export function ToolProvenance({ value }: { value: Record<string, unknown> }) {
  const source = value.source;
  const item = source && typeof source === "object" && !Array.isArray(source) ? source as Record<string, unknown> : null;
  const knownSource = item && (item.kind === "http" || item.kind === "mcp") && typeof item.server === "string" && typeof item.tool === "string";
  const risk = value.risk === "write" ? "写操作" : value.risk === "read" ? "只读" : value.risk !== undefined || knownSource ? "风险未标注" : "";
  if (!knownSource && !risk) return null;
  return <p data-ui-data className="chat-run-reference">
    {knownSource && <>{item.kind === "mcp" ? "MCP" : "HTTP"} · {String(item.server)} / {String(item.tool)}{risk && " · "}</>}{risk}
  </p>;
}
