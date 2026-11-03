import { Children, isValidElement, useEffect, useRef, useState, type ReactNode } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Check, Copy } from "lucide-react";
import { Button } from "../components/ui/button";

function CodeBlock({ children }: { children?: ReactNode }) {
  const code = Children.toArray(children)[0];
  const props = isValidElement<{ children?: ReactNode; className?: string }>(code) ? code.props : undefined;
  const text = typeof props?.children === "string" ? props.children : "";
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);
  const timer = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(timer.current), []);

  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true); setFailed(false);
      window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => setCopied(false), 1800);
    } catch { setFailed(true); }
  }

  return <div className="p-code-block">
    <Button variant="ghost" size="icon-xs" className="p-copy-code" aria-label="复制代码" title={copied ? "已复制" : failed ? "复制失败" : "复制代码"} onClick={() => void copy()}>
      {copied ? <Check /> : <Copy />}
      <span className="sr-only">{copied ? "已复制" : failed ? "复制失败" : "复制代码"}</span>
    </Button>
    <pre>{children}</pre>
  </div>;
}

export function MarkdownMessage({ text }: { text: string }) {
  return <div className="p-markdown"><ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml components={{
    pre: CodeBlock,
    table: ({ children }) => <div className="p-table-scroll" tabIndex={0} role="region" aria-label="表格"><table>{children}</table></div>,
    a: ({ children, href }) => <a href={href} target="_blank" rel="noreferrer">{children}</a>,
  }}>{text}</ReactMarkdown></div>;
}
