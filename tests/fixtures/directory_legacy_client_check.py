"""用此前已分发安装包真实读取升级服务的普通目录提案。"""

import json
import sys

import krim_agent
from krim_agent.client import KrimClient
from krim_agent.sdk import DirectoryChangeSpec, DirectoryPreviewRequest

url, agent, key = sys.argv[1:]
assert "/site-packages/" in str(krim_agent.__file__)
with KrimClient(url, key) as client:
    proposal = client.agent_directory_preview(
        agent,
        DirectoryPreviewRequest(
            spec=DirectoryChangeSpec(path="legacy-empty", operation="create", mode=0o750),
            request_key="legacy-preview",
        ),
    )
    assert proposal.operation == "create"
    found = client.agent_directory_query(
        agent, kind="proposal_by_key", request_key="legacy-preview"
    ).proposal
    assert found == proposal
    assert client.agent_directory_query(agent).proposals == [proposal]
    print(json.dumps({"legacy_package": krim_agent.__file__, "ordinary_response_compatible": True}))
