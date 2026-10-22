import { useEffect, useRef, useState } from "react";
import { request } from "../api";
import { parseArtifactChunk, type ArtifactRevision } from "../lib/workspace-contracts";
import { workBase, type WorkScope } from "../lib/workspace-requests";
import { Button } from "./ui/button";

export function ArtifactView({ value, scope, apiKey, capture }: { value: ArtifactRevision;
  scope: WorkScope; apiKey: string; capture: () => () => boolean }) {
  const [text, setText] = useState(""), [notice, setNotice] = useState("");
  const reader = useRef<AbortController | null>(null), epoch = useRef(0);
  useEffect(() => { epoch.current++; setText(""); return () => {
    epoch.current++; reader.current?.abort(); }; }, [value.revision_id, apiKey, scope.session_id]);
  async function read(path: string, offset = 0) {
    reader.current?.abort(); const controller = new AbortController(); reader.current = controller;
    const current = capture(), generation = ++epoch.current;
    try {
      const chunk = parseArtifactChunk(await request(workBase(scope)+"/artifacts/"+
        encodeURIComponent(value.revision_id)+"/files/"+path.split("/").map(encodeURIComponent).join("/")+
        "?offset="+offset+"&length=16384", apiKey, undefined, controller.signal));
      if (!current() || generation !== epoch.current || controller.signal.aborted) return;
      if (chunk.revision_id !== value.revision_id || chunk.path !== path || chunk.offset !== offset)
        throw new Error("分片身份不符");
      setText(new TextDecoder("utf-8", { fatal: true }).decode(
        Uint8Array.from(atob(chunk.data), character => character.charCodeAt(0)))); setNotice("");
    } catch { if (current() && generation === epoch.current) setNotice("无法读取此文本分片"); }
  }
  return <section className="workspace-artifact" aria-label="产物版本">
    <h3>产物版本 {value.revision}</h3><code data-ui-data>{value.manifest_hash}</code>
    <ul>{value.manifest.map(file => <li key={file.path}><Button variant="ghost" onClick={() =>
      void read(file.path)}>{file.path}</Button><span>{file.size} 字节</span></li>)}</ul>
    {notice && <p role="status">{notice}</p>}<pre tabIndex={0} data-ui-data>{text}</pre>
  </section>;
}
