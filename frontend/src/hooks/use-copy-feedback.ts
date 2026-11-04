import { useCallback, useEffect, useRef, useState } from "react";

export function useCopyFeedback() {
  const [feedback, setFeedback] = useState({ message: "", error: false, sequence: 0 });
  const timer = useRef<number | undefined>(undefined);
  const mounted = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; window.clearTimeout(timer.current); };
  }, []);

  const copy = useCallback(async (text: string): Promise<boolean> => {
    let succeeded = false;
    try { await navigator.clipboard.writeText(text); succeeded = true; }
    catch { /* 浏览器可以拒绝剪贴板写入，保留手动复制入口。 */ }
    if (mounted.current) {
      setFeedback(value => ({ message: succeeded ? "已复制" : "复制失败，请选择文字后复制", error: !succeeded, sequence: value.sequence + 1 }));
      window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => {
        if (mounted.current) setFeedback(value => ({ ...value, message: "" }));
      }, succeeded ? 2200 : 5000);
    }
    return succeeded;
  }, []);

  return { ...feedback, copy };
}
