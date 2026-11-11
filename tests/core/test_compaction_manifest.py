"""真实prepare路径的前后计量；不用摘要模型输入冒充业务上下文。"""
from types import SimpleNamespace

from krim_agent.context.compaction import Compactor
from krim_agent.context.types import CompactionPolicy, ContextPolicy
from krim_agent.contracts import Message, encoded, fingerprint
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep


class Connection:
    async def execute(self, _statement):
        return None


class Store:
    async def work_read(self, read):
        return await read(Connection())

    async def work_write(self, write, _known):
        return await write(Connection())


class Provider:
    async def stream(self, messages, tools):
        yield ModelText(text="answer")


class MeasuredCompactor(Compactor):
    """Only persistence/summary execution is substituted; prepare/measure/compose are real."""

    def __init__(self):
        self.store = Store()
        self.check = lambda: None
        self.system = "system"
        self.policy = ContextPolicy(max_bytes=8192, reserved_output_tokens=32)
        self.compaction = CompactionPolicy()
        self.request_limit = 262144
        self.step = ModelStep(Provider(), self.emit, self.check, max_steps=8, max_output=4096,
                              deadline=None, workspace=True)
        self.history = []
        for index in range(8):
            messages = [Message(role="user", content="question " + "x" * 500),
                        Message(role="assistant", content="answer " + "y" * 500)]
            ref = {"turn_id": f"turn-{index}", "run_id": f"run-{index}",
                   "terminal_seq": 4, "transcript_fingerprint": fingerprint(
                       [message.model_dump(mode="json") for message in messages])}
            self.history.append({"messages": messages, "ref": ref})
        self.anchor_refs = tuple(turn["ref"] for turn in self.history)
        self.source_anchor = fingerprint(list(self.anchor_refs))
        self.summary, self.covered, self.passes = "", 0, 0
        self.compaction_changes = []
        self.events = []
        self.summary_requests = []

    async def emit(self, kind, data):
        self.events.append((kind, data))
        return SimpleNamespace()

    def guard(self):
        return None

    async def verify(self, _conn):
        return None

    async def summarize(self, refs, messages):
        assert refs == list(self.anchor_refs[:self.candidate_end])
        self.summary_requests.append(messages)
        self.step.index += 1
        self.passes += 1
        return "a compact summary"


async def test_compaction_changes_measure_business_context_before_and_after():
    compactor = MeasuredCompactor()
    conversation = [Message(role="user", content="current user request")]
    original = [Message(role="system", content=compactor.system),
                *[message for turn in compactor.history for message in turn["messages"]],
                *conversation]
    expected_before, _ = compactor.measure(original, [], 2, 10)
    prepared = await compactor.prepare("run", conversation, [], max_group=2, remaining_tools=10)
    assert len(compactor.compaction_changes) == 1
    change = compactor.compaction_changes[0]
    assert change["before_input_bytes"] == expected_before
    assert change["pass_index"] == 0 and change["summary_step_index"] == 0
    assert 0 < change["after_input_bytes"] < expected_before
    assert change["after_input_bytes"] == compactor.measure(prepared, [], 2, 10)[0]
    summary_request_bytes = len(encoded({
        "messages": [m.model_dump(mode="json") for m in compactor.summary_requests[0]],
        "tools": [],
    }).encode())
    assert change["before_input_bytes"] != summary_request_bytes
    await compactor.step.call(prepared, [], max_group=2, remaining_tools=10,
                              compaction_changes=compactor.compaction_changes)
    manifest = next(data["context_manifest"] for kind, data in compactor.events
                    if kind == "model_start")
    assert manifest["compaction_changes"] == [change]
    assert manifest["input_bytes"] == change["after_input_bytes"]
    assert "a compact summary" not in encoded(manifest)
    # A subsequent request which reuses the summary did not perform another compression.
    await compactor.prepare("run", conversation, [], max_group=2, remaining_tools=10)
    assert compactor.compaction_changes == []
