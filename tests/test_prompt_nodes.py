import pytest
from pydantic import ValidationError

from krim_agent.prompt_context import MentionInput
from krim_agent.prompt_nodes import (
    PromptMentionNode,
    PromptSkillNode,
    PromptTextNode,
    validate_prompt_nodes,
)
from krim_agent.protocol.domain import ChatRequest, EditRequest, RegenerateRequest, ServiceError
from krim_agent.skills import SkillBatch, SkillDefinition


async def test_structured_nodes_roundtrip_mention_skill_snapshot_and_idempotency(repository):
    topic = await repository.create_topic("节点")
    mention = await repository.add_mention(topic.id, MentionInput(label="doc", content="明确内容"))
    await repository.inject_skills(
        topic.id,
        SkillBatch(skills=[SkillDefinition(name="review", description="review", content="body")]),
    )
    nodes = [
        PromptTextNode(text="请看 "),
        PromptMentionNode(mention_id=mention.id, label="doc"),
        PromptTextNode(text="。"),
        PromptSkillNode(name="review"),
    ]
    request = ChatRequest(
        text="请看 @doc。",
        idempotency_key="nodes",
        mentions=[{"mention_id": mention.id, "start": 3, "end": 7}],
        skill_names=["review"],
        nodes=nodes,
    )
    run, created = await repository.begin_run(topic.id, request)
    assert created
    message = (await repository.list_messages(topic.id))[0]
    assert message.details["prompt_nodes"] == [node.model_dump(mode="json") for node in nodes]
    same, duplicate = await repository.begin_run(topic.id, request)
    assert not duplicate and same.id == run.id


@pytest.mark.parametrize(
    "nodes",
    [
        [PromptTextNode(text="wrong")],
        [PromptMentionNode(mention_id="missing", label="doc")],
        [PromptTextNode(text="请看 "), PromptMentionNode(mention_id="x", label="wrong")],
        [PromptTextNode(text="请看 @doc"), PromptSkillNode(name="review")],
    ],
)
async def test_invalid_structured_nodes_are_rejected_without_run(repository, nodes):
    topic = await repository.create_topic("节点错误")
    request = ChatRequest(text="请看 @doc", idempotency_key=str(len(nodes)), nodes=nodes)
    with pytest.raises(ServiceError) as error:
        await repository.begin_run(topic.id, request)
    assert error.value.code == "prompt_nodes_invalid"
    assert not await repository.list_messages(topic.id)


def test_node_contract_rejects_duplicate_or_malformed_skill_and_control_text():
    with pytest.raises(ValidationError):
        PromptSkillNode(name="Review")
    with pytest.raises(ValidationError):
        PromptTextNode(text="\x00")
    with pytest.raises(ValueError):
        validate_prompt_nodes("text", [PromptTextNode(text="other")])


async def test_node_edit_and_regeneration_preserve_correct_user_snapshot(repository):
    topic = await repository.create_topic("节点分支")
    original, _ = await repository.begin_run(
        topic.id,
        ChatRequest(text="原文", idempotency_key="original", nodes=[PromptTextNode(text="原文")]),
    )
    await repository.finish(original.id, "completed", None, [])
    edited, _ = await repository.begin_run(
        topic.id,
        EditRequest(text="编辑", idempotency_key="edit", nodes=[PromptTextNode(text="编辑")]),
        action="edit",
        source_message_id=original.input_message_id,
    )
    await repository.finish(edited.id, "completed", None, [])
    regenerated, _ = await repository.begin_run(
        topic.id,
        RegenerateRequest(idempotency_key="regenerate"),
        action="regenerate",
        source_message_id=original.input_message_id,
    )
    tree = await repository.message_tree(topic.id)
    original_message = next(item for item in tree.messages if item.id == original.input_message_id)
    edited_message = next(item for item in tree.messages if item.id == edited.input_message_id)
    assert regenerated.input_message_id == original.input_message_id
    assert original_message.details["prompt_nodes"] == [{"type": "text", "text": "原文"}]
    assert edited_message.details["prompt_nodes"] == [{"type": "text", "text": "编辑"}]
