"""真实token计量与1M/900k窗口常量。

计数优先级：协议usage校正（响应后的真实prompt_tokens）> 成熟tokenizer
（tiktoken，仅当环境已安装且模型确有对应编码）> 分段启发估计。
启发估计声明精度（保守上偏，见 ``HeuristicTokenCounter``），绝不以
UTF-8字节数除固定常数冒充精确计数；``approx-utf8/1`` 仅作为旧冻结
策略的兼容标记保留，不再生成新策略。
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from typing import Any, Protocol, cast

from krim_agent.context.serialization import chunks

# 计划第三节固定的窗口与触发点；当前不提供手动阈值入口。
CONTEXT_WINDOW_TOKENS = 1_000_000
COMPACTION_TRIGGER_TOKENS = 900_000

LEGACY_COUNTER_VERSION = "approx-utf8/1"
TOKEN_WINDOW_VERSION = "token-window/1"
HEURISTIC_COUNTER_VERSION = "heuristic-script/1"

# 面向o200k/cl100k系BPE的保守分段比率；精度声明见各计数器docstring。
_CJK_RUN = re.compile(
    "[⺀-⻿⼀-⿟　-〿㇀-㇯㈀-㋿"
    "㐀-䶿一-鿿豈-﫿︰-﹏＀-￯"
    "぀-ヿㇰ-ㇿᄀ-ᇿ㄰-㆏가-힯]+"
)
_ASTRAL_RUN = re.compile("[\U00010000-\U0010ffff]+")
_NON_ASCII_RUN = re.compile("[^\x00-\x7f]+")
_ASCII_TOKENS_PER_CHAR = 10 / 32  # 1 token约3.2个ASCII字符；对JSON结构偏保守
_OTHER_TOKENS_PER_CHAR = 1.0
_ASTRAL_TOKENS_PER_CHAR = 2.0


def _request_value(messages: Sequence[Any], tools: Sequence[Any]) -> dict[str, Any]:
    """与builder.byte_size一致的规范JSON投影，计数和字节计量同一对象。"""
    return {
        "messages": [
            message.model_dump(mode="json") if hasattr(message, "model_dump") else message
            for message in messages
        ],
        "tools": list(tools),
    }


class TokenCounter(Protocol):
    """一次请求/一段文本的token计数端口；宿主可注入合成计数器做边界测试。"""

    @property
    def version(self) -> str: ...

    @property
    def exact(self) -> bool: ...

    def count_text(self, text: str) -> int: ...

    def count_request(
        self,
        messages: Sequence[Any],
        tools: Sequence[Any],
    ) -> int: ...


class HeuristicTokenCounter:
    """分段字符估计器；无本地实验校准，接口按估计语义使用。

    计数方式（``heuristic-script/1``）：CJK（汉字/假名/谚文/全角及CJK标点）
    按1 token/字符，Emoji等星面字符按2 token/字符，其余非ASCII按1
    token/字符，ASCII（含JSON结构、代码、拉丁文本）按1 token/3.2字符分段
    累加。设计方向对散文/结构化JSON偏上偏（字符率按较稠密词元化取值），
    但本模块未做任何实测校准，不附带精度百分比或"必然安全"保证；是否
    足够保守必须由接入方用目标模型的响应usage核对（见
    ``UsageCalibratedCounter``）。这不是精确计数。
    """

    version = HEURISTIC_COUNTER_VERSION
    exact = False

    def count_text(self, text: str) -> int:
        cjk = astral = non_ascii = 0
        for run in _CJK_RUN.finditer(text):
            cjk += len(run.group())
        for run in _ASTRAL_RUN.finditer(text):
            astral += len(run.group())
        for run in _NON_ASCII_RUN.finditer(text):
            non_ascii += len(run.group())
        # 三类区间互不重叠；astral属于non_ascii，需从其中扣除。
        other = non_ascii - cjk - astral
        ascii_chars = len(text) - non_ascii
        tokens = (
            cjk
            + astral * _ASTRAL_TOKENS_PER_CHAR
            + other * _OTHER_TOKENS_PER_CHAR
            + ascii_chars * _ASCII_TOKENS_PER_CHAR
        )
        return math.ceil(tokens)

    def count_request(
        self,
        messages: Sequence[Any],
        tools: Sequence[Any],
    ) -> int:
        return self.count_text("".join(chunks(_request_value(messages, tools))))


class TiktokenCounter:
    """成熟tokenizer复用：对纯文本精确，对完整协议请求仍属估计。

    tiktoken对plain text的编码是精确的（``count_text``）；但完整请求的
    token数与"对规范JSON整体encode"并不相等——每条message/工具定义的协议
    开销、图片等非文本部件按官方计数文档另有计入方式
    （developers.openai.com/api/docs/guides/token-counting）。因此
    ``count_request`` 的结果按估计对待，本计数器整体 ``exact=False``；
    仅 ``count_text`` 可视作对纯文本的精确计数。工厂只为
    ``tiktoken.encoding_for_model`` 能解析的模型选择该计数器，兼容网关上
    其他厂商模型不会落入此路径。
    """

    exact = False

    def __init__(self, encoding: Any, encoding_name: str) -> None:
        self._encoding = encoding
        self.version = f"tiktoken-{encoding_name}/1"

    def count_text(self, text: str) -> int:
        return len(self._encoding.encode(text, disallowed_special=()))

    def count_request(
        self,
        messages: Sequence[Any],
        tools: Sequence[Any],
    ) -> int:
        return self.count_text("".join(chunks(_request_value(messages, tools))))


class UsageCalibratedCounter:
    """响应usage校正：宿主在每次响应后回填(估计值, 真实prompt_tokens)。

    EWMA比率收敛估计偏差，夹在[0.25, 4.0]内防单次异常usage放大；未校正时
    透传基础计数器。比率是进程内存状态，不持久化。注意：当前runtime尚未
    调用 ``observe``，该校正为待接入接口；接入前计数即为基础估计器的
    原始结果，不应在对外说明中声称"已经按响应usage校正"。
    """

    exact = False

    def __init__(self, base: TokenCounter, *, alpha: float = 0.25) -> None:
        self._base = base
        self._alpha = alpha
        self._ratio = 1.0
        self.observations = 0

    @property
    def version(self) -> str:
        return f"calibrated/{self._base.version}"

    @property
    def ratio(self) -> float:
        return self._ratio

    def observe(self, estimated: int, actual: int) -> None:
        """以一次响应的真实prompt_tokens校正估计比率；任一值非正则忽略。"""
        if estimated <= 0 or actual <= 0:
            return
        sample = min(max(actual / estimated, 0.25), 4.0)
        self._ratio = min(max(self._ratio * (1 - self._alpha) + sample * self._alpha, 0.25), 4.0)
        self.observations += 1

    def _scaled(self, tokens: float) -> int:
        return math.ceil(tokens * self._ratio)

    def count_text(self, text: str) -> int:
        return self._scaled(self._base.count_text(text))

    def count_request(
        self,
        messages: Sequence[Any],
        tools: Sequence[Any],
    ) -> int:
        return self._scaled(self._base.count_request(messages, tools))


def _tiktoken_module() -> Any | None:
    try:
        import tiktoken
    except ImportError:
        return None
    return tiktoken


def counter_for_model(model: str | None) -> TokenCounter:
    """按模型选择计数器：有登记编码用tiktoken精确计数，否则分段估计。

    tiktoken未安装时静默回落到 ``HeuristicTokenCounter``；新增tokenizer依赖
    需要项目协调（见pyproject集成说明），不在本模块引入硬依赖。
    """
    tiktoken = _tiktoken_module()
    if tiktoken is not None and model:
        try:
            encoding = tiktoken.encoding_for_model(model)
        except (KeyError, ValueError):
            encoding = None
        if encoding is not None:
            return TiktokenCounter(encoding, cast(str, encoding.name))
    return DEFAULT_COUNTER


DEFAULT_COUNTER = HeuristicTokenCounter()


__all__ = [
    "COMPACTION_TRIGGER_TOKENS",
    "CONTEXT_WINDOW_TOKENS",
    "DEFAULT_COUNTER",
    "HEURISTIC_COUNTER_VERSION",
    "LEGACY_COUNTER_VERSION",
    "TOKEN_WINDOW_VERSION",
    "HeuristicTokenCounter",
    "TiktokenCounter",
    "TokenCounter",
    "UsageCalibratedCounter",
    "counter_for_model",
]
