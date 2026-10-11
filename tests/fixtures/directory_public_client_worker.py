"""真实独立Client进程向原TCP服务提交同一个目录动作。"""

import os
import sys
from pathlib import Path

from krim_agent.client import KrimClient
from krim_agent.workspace_directory import DirectoryChangeAction


def main() -> None:
    url, agent_id, path = sys.argv[1:]
    request = DirectoryChangeAction.model_validate_json(Path(path).read_bytes())
    with KrimClient(url, os.environ["KRIM_AGENT_API_KEY"]) as client:
        result = client.agent_directory_change(agent_id, request, confirmed=True)
        print(result.model_dump_json())


if __name__ == "__main__":
    main()
