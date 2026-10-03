"""SRC-OSS-012：Python 最小 HTTP/SSE 客户端；只通过公开 API。"""

from __future__ import annotations

from collections.abc import Iterator
from types import TracebackType
from uuid import uuid4

import httpx
from pydantic import TypeAdapter

from .protocol.domain import (
    ChatRequest,
    EditRequest,
    MessageTreeView,
    MessageView,
    RegenerateRequest,
    RunView,
    TopicView,
)
from .protocol.models import StreamFrame


class ClientError(Exception):
    pass


class KrimClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        api_key: str = "",
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._http = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=60,
            transport=transport,
            trust_env=False,
            follow_redirects=False,
        )

    def __enter__(self) -> KrimClient:
        return self

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    @staticmethod
    def _check(response: httpx.Response) -> None:
        if response.is_success:
            return
        raise ClientError(f"HTTP 请求失败: {response.status_code}；请检查服务、权限或运行状态")

    def create_topic(self, title: str = "新话题") -> TopicView:
        response = self._http.post("/v1/topics", json={"title": title})
        self._check(response)
        return TopicView.model_validate_json(response.content)

    def topics(self) -> list[TopicView]:
        response = self._http.get("/v1/topics")
        self._check(response)
        return TypeAdapter(list[TopicView]).validate_python(response.json()["topics"])

    def messages(self, topic_id: str) -> list[MessageView]:
        response = self._http.get(f"/v1/topics/{topic_id}/messages")
        self._check(response)
        return TypeAdapter(list[MessageView]).validate_python(response.json()["messages"])

    def tree(self, topic_id: str) -> MessageTreeView:
        response = self._http.get(f"/v1/topics/{topic_id}/tree")
        self._check(response)
        return MessageTreeView.model_validate_json(response.content)

    def select_branch(self, topic_id: str, message_id: str) -> TopicView:
        response = self._http.put(f"/v1/topics/{topic_id}/branch", json={"message_id": message_id})
        self._check(response)
        return TopicView.model_validate_json(response.content)

    def edit(
        self, topic_id: str, message_id: str, text: str, idempotency_key: str | None = None
    ) -> RunView:
        request = EditRequest(text=text, idempotency_key=idempotency_key or str(uuid4()))
        response = self._http.post(
            f"/v1/topics/{topic_id}/messages/{message_id}/edit", json=request.model_dump()
        )
        self._check(response)
        return RunView.model_validate_json(response.content)

    def regenerate(
        self, topic_id: str, message_id: str, idempotency_key: str | None = None
    ) -> RunView:
        request = RegenerateRequest(idempotency_key=idempotency_key or str(uuid4()))
        response = self._http.post(
            f"/v1/topics/{topic_id}/messages/{message_id}/regenerate", json=request.model_dump()
        )
        self._check(response)
        return RunView.model_validate_json(response.content)

    def send(
        self,
        topic_id: str,
        text: str,
        idempotency_key: str | None = None,
        *,
        parent_message_id: str | None = None,
    ) -> RunView:
        request = ChatRequest(
            text=text,
            idempotency_key=idempotency_key or str(uuid4()),
            parent_message_id=parent_message_id,
        )
        response = self._http.post(f"/v1/topics/{topic_id}/messages", json=request.model_dump())
        self._check(response)
        return RunView.model_validate_json(response.content)

    def run(self, run_id: str) -> RunView:
        response = self._http.get(f"/v1/runs/{run_id}")
        self._check(response)
        return RunView.model_validate_json(response.content)

    def run_messages(self, run_id: str) -> list[MessageView]:
        response = self._http.get(f"/v1/runs/{run_id}/messages")
        self._check(response)
        return TypeAdapter(list[MessageView]).validate_python(response.json()["messages"])

    def stop(self, run_id: str) -> RunView:
        response = self._http.post(f"/v1/runs/{run_id}/stop")
        self._check(response)
        return RunView.model_validate_json(response.content)

    def events(self, run_id: str, from_seq: int = 0) -> Iterator[StreamFrame]:
        with self._http.stream(
            "GET",
            f"/v1/runs/{run_id}/events",
            params={"from_seq": from_seq},
            timeout=httpx.Timeout(30, read=45),
        ) as response:
            self._check(response)
            data: list[str] = []
            event_id = ""
            event_kind = ""
            expected = from_seq
            event_bytes = 0
            for line in response.iter_lines():
                if len(line) > 131072:
                    raise ClientError("SSE 行超过大小限制")
                if line.startswith("data:"):
                    event_bytes += len(line.encode("utf-8"))
                    if event_bytes > 131072:
                        raise ClientError("SSE 事件超过大小限制")
                    data.append(line[5:].lstrip(" "))
                elif line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("event:"):
                    event_kind = line[6:].strip()
                elif not line and data:
                    frame = StreamFrame.model_validate_json("\n".join(data))
                    if (
                        frame.stream_id != run_id
                        or frame.seq != expected
                        or event_id != str(frame.seq)
                        or event_kind != frame.kind
                    ):
                        raise ClientError("SSE 帧关联或顺序不正确")
                    yield frame
                    expected += 1
                    data, event_id, event_kind = [], "", ""
                    event_bytes = 0
                    if frame.kind in {"end", "error"}:
                        return
            raise ClientError("SSE 未收到终止帧；运行可能仍在后台执行，请读取运行状态")
