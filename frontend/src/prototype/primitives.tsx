import { type ComponentProps, type ReactNode } from "react";
import { Tooltip } from "@base-ui/react/tooltip";
import { Menu } from "@base-ui/react/menu";
import { Button } from "../components/ui/button";

export function IconButton({ label, children, className = "", ...props }: ComponentProps<typeof Button> & { label: string }) {
  return <Tooltip.Root><Tooltip.Trigger render={<Button variant="ghost" size="icon-sm" className={`p-icon ${className}`} aria-label={label} {...props} />}>
    {children}
  </Tooltip.Trigger><Tooltip.Portal><Tooltip.Positioner sideOffset={7}><Tooltip.Popup className="p-tooltip">{label}</Tooltip.Popup></Tooltip.Positioner></Tooltip.Portal></Tooltip.Root>;
}

export function ActionMenu({ label, children, trigger, className = "" }: { label: string; children: ReactNode; trigger: ReactNode; className?: string }) {
  return <Menu.Root><Menu.Trigger render={<Button variant="ghost" size="icon-sm" className={`p-icon ${className}`} aria-label={label} title={label} />}>{trigger}</Menu.Trigger>
    <Menu.Portal><Menu.Positioner sideOffset={5} align="end" className="p-menu-positioner"><Menu.Popup className="p-menu">{children}</Menu.Popup></Menu.Positioner></Menu.Portal>
  </Menu.Root>;
}

export function TextMenu({ label, children, trigger, className = "" }: { label: string; children: ReactNode; trigger: ReactNode; className?: string }) {
  return <Menu.Root><Menu.Trigger render={<Button variant="ghost" className={`p-text-menu ${className}`} aria-label={label} />}>{trigger}</Menu.Trigger>
    <Menu.Portal><Menu.Positioner sideOffset={5} align="start" className="p-menu-positioner"><Menu.Popup className="p-menu">{children}</Menu.Popup></Menu.Positioner></Menu.Portal>
  </Menu.Root>;
}

export function MenuAction({ children, ...props }: ComponentProps<typeof Menu.Item>) {
  return <Menu.Item className="p-menu-item" {...props}>{children}</Menu.Item>;
}

export function KrimMark({ className = "" }: { className?: string }) {
  return <span className={`p-krim-mark ${className}`} aria-hidden="true">k<span>·</span></span>;
}
