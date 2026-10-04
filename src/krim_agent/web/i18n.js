/** Python 提供区域解析和实际文案，浏览器只按固定键渲染。 */
import { requestJson } from "./api.js";
let effective = "zh-CN";
let strings = {};
export function t(key, values = {}) {
    const template = Object.hasOwn(strings, key) ? strings[key] : key;
    return template.replace(/\{([a-z_]+)\}/g, (original, name) => Object.hasOwn(values, name) ? String(values[name]) : original);
}
export function currentLocale() { return effective; }
export function localDate(value) { return new Date(value).toLocaleString(effective); }
export function loadLocale(requested) {
    return requestJson(`/v1/locales?locale=${encodeURIComponent(requested)}`);
}
export function applyLocale(bundle) {
    effective = bundle.effective;
    strings = bundle.strings;
    document.documentElement.lang = effective;
    for (const node of document.querySelectorAll("[data-i18n]"))
        node.textContent = t(node.dataset.i18n);
    for (const attribute of ["placeholder", "title", "aria-label"]) {
        for (const node of document.querySelectorAll(`[data-i18n-${attribute}]`)) {
            const key = node.getAttribute(`data-i18n-${attribute}`);
            node.setAttribute(attribute, t(key));
        }
    }
    const select = document.getElementById("locale-select");
    if (select)
        select.value = effective;
}
