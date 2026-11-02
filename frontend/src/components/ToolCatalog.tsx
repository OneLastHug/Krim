import { useEffect, useRef, useState } from "react";
import { request } from "../api";

type ToolScope = {
  principal_id: string;
  generation: number;
  workspace: string;
  session: string;
  target: string;
};

type ToolSource = {
  kind: "http" | "mcp";
  server: string;
  tool: string;
};

type ToolEntry = {
  name: string;
  source: ToolSource;
  risk: string | null;
  inputSchema: unknown;
  outputSchema: unknown;
};

type CatalogSnapshot = { owner: string; tools: ToolEntry[] };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readTools(value: unknown): ToolEntry[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item): ToolEntry[] => {
    if (!isRecord(item) || typeof item.name !== "string" || !item.name || !isRecord(item.source)) return [];
    const source = item.source;
    if ((source.kind !== "http" && source.kind !== "mcp")
      || typeof source.server !== "string" || !source.server
      || typeof source.tool !== "string" || !source.tool) return [];
    return [{
      name: item.name,
      source: { kind: source.kind, server: source.server, tool: source.tool },
      risk: typeof item.risk === "string" ? item.risk : null,
      inputSchema: item.input_schema,
      outputSchema: item.output_schema,
    }];
  });
}

function schemaText(value: unknown): string {
  try { return JSON.stringify(value ?? null, null, 2) ?? "null"; }
  catch { return "null"; }
}

export function ToolCatalog({ apiKey, targetKey, scope, active }: {
  apiKey: string;
  targetKey: string;
  scope: ToolScope;
  active: boolean;
}) {
  const owner = JSON.stringify([
    scope.principal_id, scope.generation, scope.workspace, scope.session,
    scope.target, targetKey, apiKey, active,
  ]);
  const ownerRef = useRef(owner);
  ownerRef.current = owner;
  const epochRef = useRef(0);
  const [snapshot, setSnapshot] = useState<CatalogSnapshot | null>(null);

  useEffect(() => {
    const epoch = ++epochRef.current;
    const controller = new AbortController();
    let current = true;
    const ownsRequest = () => current && epochRef.current === epoch
      && ownerRef.current === owner && !controller.signal.aborted;

    if (!active || targetKey !== scope.target || !targetKey) {
      return () => {
        current = false;
        controller.abort();
        if (epochRef.current === epoch) epochRef.current++;
      };
    }

    void request(`/v2/targets/${encodeURIComponent(targetKey)}/tools`, apiKey, undefined, controller.signal)
      .then(value => {
        if (ownsRequest()) setSnapshot({ owner, tools: readTools(value) });
      })
      .catch(() => {
        // 目录缺失、无读取权限或请求失败时保持安静，不影响对话与审批。
        if (ownsRequest()) setSnapshot({ owner, tools: [] });
      });

    return () => {
      current = false;
      controller.abort();
      if (epochRef.current === epoch) epochRef.current++;
    };
  }, [active, apiKey, owner, scope.target, targetKey]);

  const tools = snapshot?.owner === owner ? snapshot.tools : [];
  if (!active || tools.length === 0) return null;

  return <details className="workspace-task-area tool-catalog" aria-label="外部工具目录">
    <summary><strong>外部工具</strong><span>{tools.length} 项</span></summary>
    {tools.map((tool, index) => <article className="chat-detail" key={`${tool.name}:${tool.source.kind}:${tool.source.server}:${tool.source.tool}:${index}`}>
      <div>
        <strong>{tool.name}</strong><span aria-hidden="true"> → </span>
        <span>{tool.source.server} / {tool.source.tool}</span>
      </div>
      <p>{tool.source.kind === "mcp" ? "MCP" : "HTTP"}　{tool.risk === "read" ? "只读" : tool.risk === "write" ? "写操作" : "风险未标注"}</p>
      <details>
        <summary>参数 Schema</summary>
        <div>
          <h4>输入</h4>
          <pre data-ui-data>{schemaText(tool.inputSchema)}</pre>
          <h4>输出</h4>
          <pre data-ui-data>{schemaText(tool.outputSchema)}</pre>
        </div>
      </details>
    </article>)}
  </details>;
}
