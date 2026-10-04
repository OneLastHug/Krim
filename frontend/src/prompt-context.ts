/** Prompt 文本中引用范围的 UI 维护；Python 服务端独立复核 Unicode code point。 */
import type { MentionOccurrence, MentionRef } from "./api.js";

export function shiftOccurrences(previous: string, next: string, occurrences: MentionOccurrence[]): MentionOccurrence[] {
  const before = Array.from(previous), after = Array.from(next);
  let prefix = 0, suffix = 0;
  while (prefix < before.length && prefix < after.length && before[prefix] === after[prefix]) prefix += 1;
  while (suffix < before.length - prefix && suffix < after.length - prefix && before[before.length - suffix - 1] === after[after.length - suffix - 1]) suffix += 1;
  const removedEnd = before.length - suffix;
  const change = after.length - before.length;
  return occurrences.flatMap((occurrence) => {
    if (occurrence.end <= prefix) return [{ ...occurrence }];
    if (occurrence.start >= removedEnd) return [{ ...occurrence, start: occurrence.start + change, end: occurrence.end + change }];
    return [];
  });
}

export function insertMention(value: string, start: number, end: number, reference: MentionRef, occurrences: MentionOccurrence[]): { value: string; occurrences: MentionOccurrence[]; cursor: number } {
  const token = `@${reference.label}`;
  const updated = value.slice(0, start) + token + value.slice(end);
  const next = shiftOccurrences(value, updated, occurrences);
  const position = Array.from(value.slice(0, start)).length;
  next.push({ mention_id: reference.id, start: position, end: position + Array.from(token).length });
  next.sort((a, b) => a.start - b.start);
  return { value: updated, occurrences: next, cursor: start + token.length };
}
