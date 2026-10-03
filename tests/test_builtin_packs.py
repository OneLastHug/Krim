from krim_agent.tools import BuiltinPackState, ToolContext, ToolRegistry, builtin_pack


async def invoke(registry, name, arguments):
    async def allowed(_definition, _context):
        return "allow"

    return await registry.invoke(name, arguments, ToolContext(registry, policy=allowed))


async def test_plan_todo_question_and_skills_are_stateful_but_not_user_answers():
    state = BuiltinPackState()
    definitions, returned = builtin_pack(state)
    assert returned is state
    registry = ToolRegistry()
    registry.register_many(*definitions)
    try:
        plan = await invoke(registry, "plan", {"operation": "replace", "items": ["分析", "验证"]})
        assert plan.data == {"items": ["分析", "验证"], "operation": "replace"}
        done = await invoke(registry, "plan", {"operation": "complete", "item": "分析"})
        assert done.data["items"] == ["验证"]
        todo = await invoke(registry, "todo", {"operation": "add", "item": "写测试"})
        item_id = todo.data["items"][0]["id"]
        assert (await invoke(registry, "todo", {"operation": "complete", "item_id": item_id})).data[
            "items"
        ][0]["completed"]
        question = await invoke(registry, "question", {"question": "选择环境", "options": ["本地"]})
        assert question.data["status"] == "pending" and len(state.pending_questions) == 1
        skill = await invoke(
            registry, "skills", {"operation": "put", "name": "review", "content": "检查"}
        )
        assert skill.data == {"skills": ["review"]}
        assert (await invoke(registry, "skills", {"operation": "get", "name": "review"})).data[
            "content"
        ] == "检查"
    finally:
        await registry.close()


async def test_pack_errors_are_explicit_and_write_permissions_still_apply():
    definitions, _state = builtin_pack()
    registry = ToolRegistry()
    registry.register_many(*definitions)
    try:
        denied = await registry.invoke(
            "plan", {"operation": "replace", "items": ["blocked"]}, ToolContext(registry)
        )
        assert denied.is_error and denied.code == "permission_denied"
        for name, payload, code in (
            ("plan", {"operation": "complete", "item": "missing"}, "plan_item_not_found"),
            ("todo", {"operation": "remove", "item_id": "missing"}, "todo_not_found"),
            ("skills", {"operation": "remove", "name": "missing"}, "skill_not_found"),
        ):
            result = await invoke(registry, name, payload)
            assert result.is_error and result.code == code
        invalid = await invoke(registry, "skills", {"operation": "get"})
        assert invalid.is_error and invalid.code == "invalid_input"
    finally:
        await registry.close()
