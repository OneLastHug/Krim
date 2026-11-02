export function ToolProvenance({ value }: { value: Record<string, unknown> }) {
  const source = value.source;
  if (!source || typeof source !== "object" || Array.isArray(source)) return null;
  const item = source as Record<string, unknown>;
  if ((item.kind !== "http" && item.kind !== "mcp")
    || typeof item.server !== "string" || typeof item.tool !== "string") return null;
  return <p data-ui-data className="chat-run-reference">
    {item.kind === "mcp" ? "MCP" : "HTTP"} · {item.server} / {item.tool} · {value.risk === "write" ? "写操作" : value.risk === "read" ? "只读" : "风险未标注"}
  </p>;
}
