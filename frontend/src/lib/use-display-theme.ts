import { useLayoutEffect, useState } from "react";

const STORAGE_KEY = "krim.display.theme/2";
const LEGACY_STORAGE_KEY = "krim.display.theme/1";
export type DisplayTheme = "warm" | "bright" | "dark";

function readTheme(): DisplayTheme {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved === "warm" || saved === "bright" || saved === "dark") return saved;
    const legacy = localStorage.getItem(LEGACY_STORAGE_KEY);
    if (legacy === "dark") return "dark";
    if (legacy === "light") return "bright";
  } catch { /* 浏览器禁用存储时仍使用默认主题。 */ }
  return "bright";
}

function applyTheme(theme: DisplayTheme) {
  document.documentElement.classList.toggle("dark", theme === "dark");
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme === "dark" ? "dark" : "light";
}

function persistTheme(theme: DisplayTheme) {
  try {
    localStorage.setItem(STORAGE_KEY, theme);
    localStorage.setItem(LEGACY_STORAGE_KEY, theme === "dark" ? "dark" : "light");
  } catch { /* 本次页面继续使用所选主题。 */ }
}

export function useDisplayTheme() {
  const [theme, setTheme] = useState<DisplayTheme>(readTheme);
  useLayoutEffect(() => { applyTheme(theme); persistTheme(theme); }, [theme]);
  function selectTheme(next: DisplayTheme) {
    setTheme(next);
    applyTheme(next);
    persistTheme(next);
  }
  return { theme, selectTheme };
}
