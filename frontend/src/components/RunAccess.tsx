import { ui } from "../lib/copy";
import { useState } from "react";
import { KeyRound, ArrowUpRight } from "lucide-react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Dialog, DialogTrigger, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";
export function RunAccess({ apiKey, onKey, onOpen, keyOpen, setKeyOpen, isLegacy = true }: {
    apiKey: string;
    onKey: (key: string) => void;
    onOpen: (id: string) => void;
    keyOpen: boolean;
    setKeyOpen: (open: boolean) => void;
    isLegacy?: boolean;
}) {
    const [open, setOpen] = useState(false), [id, setId] = useState(""), [draft, setDraft] = useState(apiKey);
    return <nav aria-label={ui.taskActions} className="access-actions">
  {isLegacy && <Dialog open={open} onOpenChange={setOpen}>
   <DialogTrigger render={<Button variant="ghost" className="access-open-task"/>}><ArrowUpRight aria-hidden="true"/>{ui.openTask}</DialogTrigger>
   <DialogContent aria-describedby={undefined}><DialogHeader><DialogTitle>{ui.openTask}</DialogTitle></DialogHeader>
    <form onSubmit={e => { e.preventDefault(); if (id.trim()) {
        onOpen(id.trim());
        setOpen(false);
    } }}>
     <Label htmlFor="history-id">{ui.taskID}</Label><Input id="history-id" value={id} onChange={e => setId(e.target.value)} placeholder={ui.pasteID} required autoComplete="off"/>
     <div className="dialog-actions"><Button type="submit">{ui.open}</Button></div>
    </form>
   </DialogContent>
  </Dialog>}
  <Dialog open={keyOpen} onOpenChange={next => { if (next)
        setDraft(apiKey); setKeyOpen(next); }}>
   <DialogTrigger render={<Button variant="outline" className="access-connect"/>}><KeyRound aria-hidden="true"/>{ui.connect}</DialogTrigger>
   <DialogContent aria-describedby={undefined}><DialogHeader><DialogTitle>{ui.connect}</DialogTitle></DialogHeader>
    <form onSubmit={e => { e.preventDefault(); onKey(draft); setKeyOpen(false); }}>
     <Label htmlFor="api-key">{ui.key}</Label><Input id="api-key" type="password" value={draft} onChange={e => setDraft(e.target.value)} autoComplete="off"/>
     <div className="dialog-actions"><Button id="connect-save" type="submit">{ui.apply}</Button></div>
    </form>
   </DialogContent>
  </Dialog>
 </nav>;
}
