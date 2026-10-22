"""Claude独立适配器：自己的argv/profile/schema，复用聚合资源阶段。"""

from .claude_events import ClaudeEvents
from .codex_cli import NativeCLIAdapter


class ClaudeCodeAdapter(NativeCLIAdapter):
    family = "claude"
    codec_type = ClaudeEvents
