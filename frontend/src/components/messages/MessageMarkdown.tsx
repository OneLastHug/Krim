import { memo, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { Element, ElementContent, Root } from "hast";
import ReactMarkdown, { type Components as MarkdownComponents } from "react-markdown";
import rehypeHighlight from "rehype-highlight";
import remarkGfm from "remark-gfm";
import { appendMarkdownBlocks, type MarkdownBlockState } from "../../lib/markdown-blocks";
import { Check, Copy } from "lucide-react";
import { Button } from "../ui/button";

import "./messages.css";

type CodeData = NonNullable<Element["data"]> & { krimCodeText?: string; krimCodeLabel?: string };

function collectText(node: ElementContent): string {
  if (node.type === "text") return node.value;
  if (node.type === "element") return node.children.map(collectText).join("");
  return "";
}

function rehypePreserveCodeText() {
  return (tree: Root) => {
    function visit(node: Root | Element) {
      if (node.type === "element" && node.tagName === "pre") {
        const code = node.children.find((child): child is Element => child.type === "element" && child.tagName === "code");
        if (code) {
          const language = Array.isArray(code.properties.className) ? code.properties.className.find(value => typeof value === "string" && value.startsWith("language-"))?.toString().slice(9) : undefined;
          const meta = typeof code.data?.meta === "string" ? code.data.meta.trim() : "";
          node.data = { ...node.data, krimCodeText: code.children.map(collectText).join(""), krimCodeLabel: meta || language || "代码" } as CodeData;
        }
      }
      for (const child of node.children) if (child.type === "element") visit(child);
    }
    visit(tree);
  };
}

function CodeBlock({ children, node, onCopy }: {
  children?: ReactNode;
  node?: Element;
  onCopy: (text: string) => Promise<boolean>;
}) {
  const text = (node?.data as CodeData | undefined)?.krimCodeText ?? "";
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);
  const timer = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(timer.current), []);
  useEffect(() => {
    setCopied(false);
    setFailed(false);
    window.clearTimeout(timer.current);
  }, [text]);

  async function copy() {
    const succeeded = await onCopy(text);
    setCopied(succeeded);
    setFailed(!succeeded);
    window.clearTimeout(timer.current);
    if (succeeded) timer.current = window.setTimeout(() => setCopied(false), 1800);
  }

  return <div className="message-code-block">
    <div className="message-code-head"><span>{(node?.data as CodeData | undefined)?.krimCodeLabel ?? "代码"}</span>
      <Button variant="ghost" size="icon-xs" className="message-copy-code chat-message-action" aria-label="复制代码" title={copied ? "已复制" : failed ? "复制失败" : "复制代码"} onClick={() => void copy()}>
        {copied ? <Check /> : <Copy />}
        <span className="sr-only">{copied ? "已复制" : failed ? "复制失败" : "复制代码"}</span>
      </Button>
    </div>
    <pre tabIndex={0} aria-label="代码片段">{children}</pre>
  </div>;
}

/**
 * 流式模式下已完成块的渲染单元：source 不变时 memo 直接跳过，
 * 不再走解析、rehype 与高亮；defs 为全文链接引用定义（渲染前前置）。
 */
const BlockMarkdown = memo(function BlockMarkdown({ source, defs, components }: { source: string; defs: string; components: MarkdownComponents }) {
  return <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypePreserveCodeText, [rehypeHighlight, { detect: false }]]} skipHtml components={components}>{defs ? `${defs}\n\n${source}` : source}</ReactMarkdown>;
});

export const MessageMarkdown = memo(function MessageMarkdown({ text, onCopy, streaming = false }: { text: string; onCopy: (text: string) => Promise<boolean>; streaming?: boolean }) {
  const components = useMemo<MarkdownComponents>(() => ({
    pre: props => <CodeBlock {...props} onCopy={onCopy} />,
    table: ({ children }) => <div className="message-table-scroll" tabIndex={0} role="region" aria-label="表格"><table>{children}</table></div>,
    a: ({ children, href, title }) => href ? <a href={href} title={title} target="_blank" rel="noreferrer">{children}</a> : <span>{children}</span>,
    img: ({ alt, src }) => typeof src === "string" && src ? <a href={src} target="_blank" rel="noreferrer">{alt || "打开图片"}</a> : <span>{alt || "图片地址不可用"}</span>,
  }), [onCopy]);
  const blockRef = useRef<MarkdownBlockState | null>(null);
  let blocks: MarkdownBlockState | null = null;
  if (streaming) {
    const cached = blockRef.current;
    if (cached !== null && cached.text === text) {
      blocks = cached;
    } else {
      blocks = appendMarkdownBlocks(cached, text);
      blockRef.current = blocks;
    }
  }
  const state = blocks;
  if (state === null || state.whole) {
    return <div className="message-markdown"><ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypePreserveCodeText, [rehypeHighlight, { detect: false }]]} skipHtml components={components}>{text}</ReactMarkdown></div>;
  }
  // 复现整段渲染 mdast-util-to-hast 的根级换行：相邻被计入的块之间插一个换行文本节点，
  // hidden（链接定义）完全不参与；ghost（HTML 块）渲染为空但分隔符仍在。
  const children: ReactNode[] = [];
  let counted = false;
  state.blocks.forEach((block, index) => {
    if (block.kind === "hidden") return;
    if (counted) children.push("\n");
    children.push(<BlockMarkdown key={index} source={block.source} defs={state.defs} components={components} />);
    counted = true;
  });
  return <div className="message-markdown">{children}</div>;
});
