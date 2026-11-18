import type { DisplayTheme } from "../lib/use-display-theme";
import type { ReadingSize } from "../hooks/use-reading-size";
import { Button } from "./ui/button";
export function AppearanceSettings({ theme, onTheme, size, onSize }: { theme: DisplayTheme; onTheme: (theme: DisplayTheme) => void; size: ReadingSize; onSize: (size: ReadingSize) => void }) {
  return <section className="appearance-settings" aria-label="外观与偏好"><h2>外观与偏好</h2><section><h3>显示主题</h3><div className="appearance-cards" role="group" aria-label="选择显示主题">{([["warm", "暖纸"], ["bright", "浅色"], ["dark", "深色"]] as const).map(([id, label]) => <Button key={id} variant="outline" className="appearance-card" aria-pressed={theme === id} onClick={() => onTheme(id)}><span className={`theme-swatch theme-swatch-${id}`} aria-hidden="true"/>{label}</Button>)}</div></section><section><h3>阅读字号</h3><div className="theme-settings" role="group" aria-label="消息字号">{([12, 14, 16] as const).map(value => <Button key={value} variant={size === value ? "secondary" : "outline"} aria-pressed={size === value} onClick={() => onSize(value)}>{value} px</Button>)}</div></section></section>;
}
