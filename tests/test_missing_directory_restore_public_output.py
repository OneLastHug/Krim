"""公开SDK/HTTP/TCP Client长UTF-8差异原64KiB完整分页，零发布。"""

import asyncio

from test_missing_directory_restore_control import checkpoint, drain, setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.protocol.models import check_json
from krim_agent.sdk import DirectoryRestoreTarget, MissingDirectoryRestorePreview


async def test_missing_public_long_diff_keeps_all_original_entries_under_json_budget(
    runtime, tmp_path, monkeypatch
):
    (
        service,
        agent,
        _,
    ) = await setup(runtime, tmp_path)
    try:
        folder = tmp_path / "tree" / ("甲" * 50) / ("乙" * 50)
        folder.mkdir(parents=True)
        for index in range(128):
            (folder / ("文" * 50 + f"-{index:03d}")).write_bytes(b"\0\xff")
        captured = await checkpoint(agent)
        (tmp_path / "tree").rename(tmp_path / "retained-original")
        planned = await agent.preview_missing_directory_checkpoint_restore(
            MissingDirectoryRestorePreview(
                target=DirectoryRestoreTarget(identifier=captured.checkpoint_id),
                request_key="public-long-diff",
            )
        )
        planned = await agent.wait_missing_directory_checkpoint_restore(
            planned.plan_id, timeout_seconds=10
        )
        await drain(service)
        assert planned.phase == "ready"
        offset, seen, pages = 0, [], 0
        async with api_for(runtime, service, monkeypatch) as (app, api), tcp_for(app) as url:
            while True:
                page = await agent.missing_directory_checkpoint_restore_diff(
                    planned.plan_id, offset=offset, limit=128
                )
                check_json(page.model_dump(mode="json"))
                response = await api.get(
                    f"/v1/agents/{agent.agent_id}/workspace/missing-directory-restores/{planned.plan_id}/diff",
                    params={"offset": offset, "limit": 128},
                )
                assert response.status_code == 200 and response.json() == page.model_dump(
                    mode="json"
                ), response.text

                def client_read(current_offset=offset):
                    with KrimClient(url) as client:
                        return client.agent_workspace_missing_directory_restore_diff(
                            agent.agent_id, planned.plan_id, offset=current_offset, limit=128
                        )

                assert await asyncio.to_thread(client_read) == page
                seen.extend(entry.path for entry in page.entries)
                pages += 1
                if page.next_offset is None:
                    break
                assert page.next_offset == offset + len(page.entries) > offset
                offset = page.next_offset
        assert pages >= 2 and len(seen) == len(set(seen)) == planned.target_snapshot.entry_count
        assert seen == sorted(seen) and not (tmp_path / "tree").exists()
    finally:
        await service.close()
