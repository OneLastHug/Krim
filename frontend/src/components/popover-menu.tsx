import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";

export function PopoverMenu({ open, anchor, onClose, placement = "below", className = "", label, children }: {
  open: boolean;
  anchor: HTMLElement | null;
  onClose: () => void;
  placement?: "below" | "above";
  className?: string;
  label: string;
  children: ReactNode;
}) {
  const menuRef = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null);

  useLayoutEffect(() => {
    if (!open || !anchor || !menuRef.current) return;
    const rect = anchor.getBoundingClientRect();
    const menu = menuRef.current.getBoundingClientRect();
    const left = Math.min(Math.max(8, rect.left), window.innerWidth - menu.width - 8);
    const top = placement === "above"
      ? Math.max(8, rect.top - menu.height - 8)
      : Math.min(rect.bottom + 8, window.innerHeight - menu.height - 8);
    setPosition({ left, top });
  }, [open, anchor, placement]);

  useEffect(() => {
    if (!open) return;
    function onPointerDown(event: PointerEvent) {
      const target = event.target as Node;
      if (menuRef.current?.contains(target) || anchor?.contains(target)) return;
      onClose();
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }
    window.addEventListener("pointerdown", onPointerDown, true);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown, true);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [open, anchor, onClose]);

  if (!open) return null;
  return <div ref={menuRef} className={`popover-menu ${className}`} role="menu" aria-label={label} style={position ? { left: position.left, top: position.top } : undefined}>
    {children}
  </div>;
}
