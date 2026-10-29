import { useEffect, useState } from "react";

const STORAGE_KEY = "krim.display.theme/1";
export type DisplayTheme = "light" | "dark";

function readTheme(): DisplayTheme {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved === "light" || saved === "dark") return saved;
  } catch { /* 浏览器禁用存储时仍允许本次切换。 */ }
  return typeof matchMedia === "function" && matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function applyTheme(theme: DisplayTheme) {
  document.documentElement.classList.toggle("dark", theme === "dark");
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
}

export function useDisplayTheme() {
  const [theme, setTheme] = useState<DisplayTheme>(readTheme);
  useEffect(() => { applyTheme(theme); }, [theme]);
  function toggleTheme() {
    setTheme(current => {
      const next = current === "dark" ? "light" : "dark";
      applyTheme(next);
      try { localStorage.setItem(STORAGE_KEY, next); } catch { /* 本次页面继续使用所选主题。 */ }
      return next;
    });
  }
  return { theme, toggleTheme };
}
