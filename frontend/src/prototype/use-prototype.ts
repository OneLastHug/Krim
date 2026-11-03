import { useEffect, useMemo, useState } from "react";
import { createFixtures, responseFor, type PrototypeConversation, type PrototypeTurn } from "./fixtures";

interface StreamJob { conversationId: string; turnId: string; answer: string }

export function usePrototype() {
  const [conversations, setConversations] = useState(() => [createFixtures()[0]!]);
  const [navigation, setNavigation] = useState({ ids: ["new"], index: 0 });
  const [job, setJob] = useState<StreamJob | null>(null);
  const currentId = navigation.ids[navigation.index] ?? "new";
  const current = conversations.find(item => item.id === currentId) ?? conversations[0]!;

  function update(id: string, transform: (item: PrototypeConversation) => PrototypeConversation) {
    setConversations(items => items.map(item => item.id === id ? transform(item) : item));
  }

  useEffect(() => {
    if (!job) return;
    let offset = 0;
    const timer = window.setInterval(() => {
      offset = Math.min(job.answer.length, offset + 12);
      const complete = offset === job.answer.length;
      setConversations(items => items.map(item => item.id !== job.conversationId ? item : {
        ...item,
        turns: item.turns.map(turn => turn.id !== job.turnId ? turn : {
          ...turn, answer: job.answer.slice(0, offset), status: complete ? "complete" : "streaming",
        }),
      }));
      if (complete) { window.clearInterval(timer); setJob(null); }
    }, 36);
    return () => window.clearInterval(timer);
  }, [job]);

  function navigate(id: string) {
    setNavigation(value => value.ids[value.index] === id ? value : {
      ids: [...value.ids.slice(0, value.index + 1), id], index: value.index + 1,
    });
  }

  function createConversation(project = "Krim") {
    const id = crypto.randomUUID();
    setConversations(items => [{ id, title: "新会话", project, pinned: false, archived: false, draft: "", turns: [] }, ...items]);
    navigate(id);
  }

  function loadExamples() {
    const examples = createFixtures().filter(item => item.id !== "new");
    setConversations(items => {
      const exampleIds = new Set(examples.map(item => item.id));
      const restored = items.map(item => exampleIds.has(item.id) ? { ...item, archived: false } : item);
      const known = new Set(restored.map(item => item.id));
      return [...restored, ...examples.filter(item => !known.has(item.id))];
    });
    navigate("timeouts");
  }

  function send(prompt = current.draft) {
    if (!prompt.trim() || job) return;
    const content = prompt.trim();
    const result = responseFor(content);
    const turn: PrototypeTurn = { id: crypto.randomUUID(), prompt: content, answer: "", status: "streaming", files: result.files };
    update(current.id, item => ({
      ...item, draft: "", archived: false,
      title: item.turns.length ? item.title : content.slice(0, 24),
      turns: [...item.turns, turn],
    }));
    setJob({ conversationId: current.id, turnId: turn.id, answer: result.answer });
  }

  function stop() {
    if (!job) return;
    update(job.conversationId, item => ({ ...item, turns: item.turns.map(turn => turn.id === job.turnId ? { ...turn, status: "stopped" } : turn) }));
    setJob(null);
  }

  function retry(turnId: string) {
    if (job) return;
    const turn = current.turns.find(item => item.id === turnId);
    if (!turn) return;
    const result = responseFor(turn.prompt);
    update(current.id, item => ({ ...item, turns: item.turns.map(value => value.id === turnId ? { ...value, answer: "", status: "streaming", files: result.files } : value) }));
    setJob({ conversationId: current.id, turnId, answer: result.answer });
  }

  function archive(id: string) {
    if (job?.conversationId === id) return;
    update(id, item => ({ ...item, archived: true }));
    if (id === current.id) createConversation(current.project);
  }

  return {
    conversations, current, busy: !!job, runningId: job?.conversationId,
    visible: useMemo(() => conversations.filter(item => !item.archived), [conversations]),
    navigate, createConversation, loadExamples, send, stop, retry, archive,
    editDraft: (draft: string) => update(current.id, item => ({ ...item, draft })),
    rename: (id: string, title: string) => { if (title.trim()) update(id, item => ({ ...item, title: title.trim() })); },
    togglePinned: (id: string) => update(id, item => ({ ...item, pinned: !item.pinned })),
    restore: (id: string) => { update(id, item => ({ ...item, archived: false })); navigate(id); },
    canBack: navigation.index > 0,
    canForward: navigation.index < navigation.ids.length - 1,
    back: () => setNavigation(value => ({ ...value, index: Math.max(0, value.index - 1) })),
    forward: () => setNavigation(value => ({ ...value, index: Math.min(value.ids.length - 1, value.index + 1) })),
  };
}

export type PrototypeState = ReturnType<typeof usePrototype>;
