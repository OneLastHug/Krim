import remarkGfm from "remark-gfm";
import remarkParse from "remark-parse";
import { unified } from "unified";

/**
 * 流式长文本的顶层块增量切分。
 *
 * 用与 MessageMarkdown 渲染端一致的解析器（remark-parse + remark-gfm）找出真实的顶层块边界，
 * 不按空行或正则近似，因此代码围栏、列表、引用、表格、Setext 标题等结构的切分点与整段解析一致。
 * 追加式更新（新文本以旧文本开头）只从上一个尾块起点重新解析，已完成块的文本原样复用；
 * 其余更新整段重新切分。
 *
 * 链接引用定义（[id]: url）在 CommonMark 中对全文生效且可出现在使用点之后，
 * 因此递归收集全文定义、按文档顺序拼成 defs，渲染每个块前统一前置，保证跨块引用与整段渲染等价。
 * 出现脚注（[^id]）这类文档级结构时置 whole，由组件回退整段渲染。
 *
 * kind 用于复现 mdast-util-to-hast 在根级子节点之间的换行分隔：
 * content 正常渲染；ghost 是 HTML 块（skipHtml 在 wrap 后才移除 raw 节点，
 * 两侧分隔符仍在但不渲染元素）；hidden 是链接定义（无 handler，渲染与分隔都不参与）。
 */

const blockParser = unified().use(remarkParse).use(remarkGfm).freeze();

interface OffsetNode {
  type: string;
  position?: { start?: { offset?: number }; end?: { offset?: number } };
  children?: OffsetNode[];
}

export type MarkdownBlockKind = "content" | "ghost" | "hidden";

export interface MarkdownBlock {
  source: string;
  kind: MarkdownBlockKind;
}

export interface MarkdownBlockState {
  /** 本次切分对应的完整文本 */
  text: string;
  /** 含脚注等文档级结构或块位置缺失时为 true，整段渲染 */
  whole: boolean;
  /** 全文链接引用定义原文（按文档顺序），渲染每个块前前置 */
  defs: string;
  /** 顶层块；除最后一个外都已完成、内容不再变化，最后一个是增长中的尾块 */
  blocks: MarkdownBlock[];
  /** 稳定区终点在全文中的偏移（尾块起点），追加更新从这里重新解析 */
  stableEnd: number;
  /** 稳定区已收集的定义原文，是 defs 的前缀 */
  stableDefs: string;
  /** 全文是否含脚注 */
  footnotes: boolean;
}

function nodeSlice(node: OffsetNode, source: string): string | null {
  const start = node.position?.start?.offset;
  const end = node.position?.end?.offset;
  return typeof start === "number" && typeof end === "number" && end >= start ? source.slice(start, end) : null;
}

function collectFacts(node: OffsetNode, source: string, facts: { defs: string[]; footnotes: boolean }): boolean {
  if (node.type === "footnoteDefinition" || node.type === "footnoteReference") facts.footnotes = true;
  if (node.type === "definition") {
    const slice = nodeSlice(node, source);
    if (slice === null) return false;
    facts.defs.push(slice);
  }
  const children = node.children;
  if (children) for (const child of children) if (!collectFacts(child, source, facts)) return false;
  return true;
}

function joinDefs(parts: string[]): string {
  const kept = parts.filter(part => part.length > 0);
  return kept.length > 0 ? kept.join("\n") : "";
}

function blockKind(node: OffsetNode): MarkdownBlockKind {
  if (node.type === "definition") return "hidden";
  if (node.type === "html") return "ghost";
  return "content";
}

export function appendMarkdownBlocks(prev: MarkdownBlockState | null, text: string): MarkdownBlockState {
  const cached = prev !== null && !prev.whole && text.startsWith(prev.text) ? prev : null;
  const base = cached ? cached.stableEnd : 0;
  const source = text.slice(base);
  const tree = blockParser.parse(source) as unknown as OffsetNode;
  const children = Array.isArray(tree.children) ? tree.children : [];
  const completed: MarkdownBlock[] = [];
  const completedDefs: string[] = [];
  let tailDefs: string[] = [];
  let footnotes = cached ? cached.footnotes : false;
  let degraded = !Array.isArray(tree.children);
  let last: OffsetNode | undefined;
  let lastKind: MarkdownBlockKind = "content";
  for (const [index, child] of children.entries()) {
    const slice = nodeSlice(child, source);
    if (slice === null) {
      degraded = true;
      break;
    }
    const facts = { defs: [] as string[], footnotes: false };
    if (!collectFacts(child, source, facts)) {
      degraded = true;
      break;
    }
    if (facts.footnotes) footnotes = true;
    last = child;
    lastKind = blockKind(child);
    if (index === children.length - 1) {
      tailDefs = facts.defs;
    } else {
      completed.push({ source: slice, kind: lastKind });
      completedDefs.push(...facts.defs);
    }
  }
  if (degraded || footnotes) {
    return { text, whole: true, defs: "", blocks: [{ source: text, kind: "content" }], stableEnd: 0, stableDefs: "", footnotes };
  }
  const stableDefs = cached ? cached.stableDefs : "";
  const stableBlocks = cached ? cached.blocks.slice(0, -1) : [];
  const mergedStableDefs = joinDefs([stableDefs, ...completedDefs]);
  const lastStart = last?.position?.start?.offset;
  const tail: MarkdownBlock = last !== undefined && typeof lastStart === "number"
    ? { source: source.slice(lastStart), kind: lastKind }
    : { source, kind: "hidden" };
  return {
    text,
    whole: false,
    defs: joinDefs([mergedStableDefs, ...tailDefs]),
    blocks: [...stableBlocks, ...completed, tail],
    stableEnd: base + (typeof lastStart === "number" ? lastStart : 0),
    stableDefs: mergedStableDefs,
    footnotes,
  };
}
