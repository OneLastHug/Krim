/** Python 提供区域解析和实际文案，浏览器只按固定键渲染。 */
import { requestJson, type LocaleView } from "./api.js";

export interface LocaleResources extends LocaleView { strings: Record<string, string>; }
let effective: "zh-CN" | "en-US" = "zh-CN";
let strings: Record<string, string> = {};

export function t(key: string, values: Record<string, string | number> = {}): string {
  const template = Object.hasOwn(strings, key) ? strings[key]! : key;
  return template.replace(/\{([a-z_]+)\}/g, (original: string, name: string) => Object.hasOwn(values, name) ? String(values[name]) : original);
}
export function currentLocale(): "zh-CN" | "en-US" { return effective; }
export function localDate(value: string): string { return new Date(value).toLocaleString(effective); }
export function loadLocale(requested: string): Promise<LocaleResources> {
  return requestJson<LocaleResources>(`/v1/locales?locale=${encodeURIComponent(requested)}`);
}
export function applyLocale(bundle: LocaleResources): void {
  effective = bundle.effective; strings = bundle.strings;
  document.documentElement.lang = effective;
  for (const node of document.querySelectorAll<HTMLElement>("[data-i18n]")) node.textContent = t(node.dataset.i18n!);
  for (const attribute of ["placeholder", "title", "aria-label"]) {
    for (const node of document.querySelectorAll<HTMLElement>(`[data-i18n-${attribute}]`)) {
      const key = node.getAttribute(`data-i18n-${attribute}`)!;
      node.setAttribute(attribute, t(key));
    }
  }
  const select = document.getElementById("locale-select") as HTMLSelectElement | null;
  if (select) select.value = effective;
}
