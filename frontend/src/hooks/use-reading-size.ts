import { useState } from "react";

export type ReadingSize = 12 | 14 | 16;
const STORAGE_KEY = "krim.display.reading-size/1";

export function useReadingSize() {
  const [size, setSize] = useState<ReadingSize>(() => {
    try {
      const value = Number(localStorage.getItem(STORAGE_KEY));
      if (value === 12 || value === 14 || value === 16) return value;
    } catch { /* 显示偏好不可保存时使用默认字号。 */ }
    return 14;
  });
  function selectSize(value: ReadingSize) {
    setSize(value);
    try { localStorage.setItem(STORAGE_KEY, String(value)); }
    catch { /* 当前页面仍应用所选字号。 */ }
  }
  return { readingSize: size, selectReadingSize: selectSize };
}
