import { memo, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { Element, ElementContent, Root } from "hast";
import ReactMarkdown, { type Components as MarkdownComponents } from "react-markdown";
import rehypeHighlight from "rehype-highlight";
import remarkGfm from "remark-gfm";
import { Check, Copy } from "lucide-react";
import { Button } from "../ui/button";

import "./messages.css";

type CodeData = NonNullable<Element["data"]> & { krimCodeText?: string };

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
        if (code) node.data = { ...node.data, krimCodeText: code.children.map(collectText).join("") } as CodeData;
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
    <Button variant="ghost" size="icon-xs" className="message-copy-code" aria-label="复制代码" title={copied ? "已复制" : failed ? "复制失败" : "复制代码"} onClick={() => void copy()}>
      {copied ? <Check /> : <Copy />}
      <span className="sr-only">{copied ? "已复制" : failed ? "复制失败" : "复制代码"}</span>
    </Button>
    <pre tabIndex={0} aria-label="代码片段">{children}</pre>
  </div>;
}

export const MessageMarkdown = memo(function MessageMarkdown({ text, onCopy }: { text: string; onCopy: (text: string) => Promise<boolean> }) {
  const components = useMemo<MarkdownComponents>(() => ({
    pre: props => <CodeBlock {...props} onCopy={onCopy} />,
    table: ({ children }) => <div className="message-table-scroll" tabIndex={0} role="region" aria-label="表格"><table>{children}</table></div>,
    a: ({ children, href, title }) => href ? <a href={href} title={title} target="_blank" rel="noreferrer">{children}</a> : <span>{children}</span>,
    img: ({ alt, src }) => typeof src === "string" && src ? <a href={src} target="_blank" rel="noreferrer">{alt || "打开图片"}</a> : <span>{alt || "图片地址不可用"}</span>,
  }), [onCopy]);
  return <div className="message-markdown"><ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypePreserveCodeText, [rehypeHighlight, { detect: false }]]} skipHtml components={components}>{text}</ReactMarkdown></div>;
});
