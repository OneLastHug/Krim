/** 文本链接的 DOM 适配；URL 裁决来自 Python，同源 iframe 只发导航通知。 */
import { requestJson } from "./api.js";
import type { HostJson } from "./host.js";
interface LinkTarget { url: string; external: boolean; }
const cache = new Map<string, Promise<LinkTarget | null>>();
export function clearLinkCache(): void { cache.clear(); }
function resolve(candidate: string): Promise<LinkTarget | null> {
  const key = location.href + "\n" + candidate;
  const current = cache.get(key); if (current) return current;
  if (cache.size >= 256) cache.delete(cache.keys().next().value!);
  const value = requestJson<LinkTarget>("/v1/links/resolve", { method: "POST", body: JSON.stringify({ url: candidate, base_url: location.href }) }).catch(() => null);
  cache.set(key, value); return value;
}
export function appendLinks(container: HTMLElement, value: string, topicId: string, messageId: string, notify: (data: HostJson) => void): void {
  const pattern = /https?:\/\/[^\s<>"']+/gi;
  let cursor = 0, count = 0;
  for (const match of value.matchAll(pattern)) {
    if (++count > 16) break;
    const start = match.index ?? 0;
    if (start > 0 && /[A-Za-z0-9_:]/.test(value[start - 1]!)) continue;
    let candidate = match[0];
    while (/[.,;:]$/.test(candidate)) candidate = candidate.slice(0, -1);
    for (const [open, close] of [["(", ")"], ["[", "]"], ["{", "}"]]) {
      while (candidate.endsWith(close!) && candidate.split(close!).length > candidate.split(open!).length) candidate = candidate.slice(0, -1);
    }
    if (!candidate || candidate.length > 2048) continue;
    container.append(document.createTextNode(value.slice(cursor, start)));
    const marker = document.createElement("span"); marker.textContent = candidate; container.append(marker);
    const displayed = candidate;
    void resolve(candidate).then((target) => {
      if (!target || !marker.isConnected) return;
      const link = document.createElement("a"); link.href = target.url; link.textContent = displayed; link.rel = "noopener noreferrer"; link.target = "_blank";
      link.addEventListener("click", (event) => {
        if (window.parent !== window) { event.preventDefault(); notify({ url: target.url, topic_id: topicId, message_id: messageId }); }
      });
      marker.replaceWith(link);
    });
    cursor = start + candidate.length;
  }
  container.append(document.createTextNode(value.slice(cursor)));
}
