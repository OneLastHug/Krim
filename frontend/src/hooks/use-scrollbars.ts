import { useEffect } from "react";

/** Keep native scrolling and a stable gutter; only reveal the thumb during scrolling. */
export function useScrollbars() {
  useEffect(() => {
    const timers = new Map<Element, ReturnType<typeof setTimeout>>();
    const onScroll = (event: Event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      target.dataset.scrolling = "true";
      clearTimeout(timers.get(target));
      timers.set(target, setTimeout(() => { delete target.dataset.scrolling; timers.delete(target); }, 800));
    };
    document.addEventListener("scroll", onScroll, { capture: true, passive: true });
    return () => {
      document.removeEventListener("scroll", onScroll, true);
      for (const [element, timer] of timers) { clearTimeout(timer); element.removeAttribute("data-scrolling"); }
      timers.clear();
    };
  }, []);
}
