"""Process-isolated SDK probe: real RunStore, SQLite receipts and a file effect.

The receipt database here is a test adapter for C0, not a production migration.
No gateway, MCP server, credential or installed app is contacted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

import ai
from ai import events, messages
from pydantic import PrivateAttr

from krim_agent.agent_sdk import ProbeStopped, SDKAdapter
from krim_agent.contracts import RunRequest, ServiceError, StrictModel, fingerprint, parse_json
from krim_agent.development.contracts import (
    EnvironmentHandle,
    OperationAdmission,
    OperationIdentity,
    OperationReceipt,
    RunOwnership,
)
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry


class ReceiptFixture:
    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, receipt TEXT)')
        self.db.commit()

    async def lookup(self, identity: OperationIdentity) -> OperationReceipt | None:
        row = self.db.execute('SELECT receipt FROM receipts WHERE id=?',
                              (identity.operation_id,)).fetchone()
        if row is None:
            return None
        receipt = OperationReceipt.model_validate_json(row[0])
        if receipt.identity != identity:
            raise ValueError('operation binding changed')
        return receipt

    async def admit(self, identity: OperationIdentity) -> OperationAdmission:
        self.db.execute('BEGIN IMMEDIATE')
        try:
            prior = await self.lookup(identity)
            if prior is not None:
                self.db.commit()
                return OperationAdmission(receipt=prior, execute=False)
            receipt = OperationReceipt(identity=identity, state='accepted')
            self.db.execute('INSERT INTO receipts VALUES (?,?)',
                            (identity.operation_id, receipt.model_dump_json()))
            self.db.commit()
            return OperationAdmission(receipt=receipt, execute=True)
        except BaseException:
            self.db.rollback()
            raise

    async def save_receipt(self, receipt: OperationReceipt) -> None:
        self.db.execute('BEGIN IMMEDIATE')
        try:
            prior = await self.lookup(receipt.identity)
            if prior is None:
                raise ValueError('receipt without admission')
            self.db.execute('UPDATE receipts SET receipt=? WHERE id=?',
                            (receipt.model_dump_json(), receipt.identity.operation_id))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise


class Args(StrictModel):
    value: int


@ai.tool
async def write(value: int) -> Any:
    return value


class Scripted(ai.Provider):
    provider_class_id: str = 'krim-cross-process-probe'
    name: str = 'process-probe'
    default_base_url: str = 'http://unused.invalid'
    _phase: str = PrivateAttr()
    _calls: int = PrivateAttr(default=0)

    def __init__(self, phase: str) -> None:
        super().__init__()
        self._phase = phase

    async def stream(self, model: Any, history: Any, **kwargs: Any) -> Any:
        self._calls += 1
        yield events.StreamStart()
        if self._phase in {'park', 'park_two'}:
            if self._calls != 1:
                raise AssertionError('park unexpectedly requested a second turn')
            yield events.ToolStart(tool_call_id='model-write', tool_name='write')
            yield events.ToolDelta(tool_call_id='model-write', chunk='{"value":7}')
            yield events.ToolEnd(tool_call_id='model-write', tool_call=messages.ToolCallPart(
                tool_call_id='model-write', tool_name='write', tool_args='{"value":7}'))
            if self._phase == 'park_two':
                yield events.ToolStart(tool_call_id='model-second', tool_name='write')
                yield events.ToolDelta(tool_call_id='model-second', chunk='{"value":8}')
                yield events.ToolEnd(tool_call_id='model-second', tool_call=messages.ToolCallPart(
                    tool_call_id='model-second', tool_name='write', tool_args='{"value":8}'))
            yield events.StreamEnd(finish_reason='tool_call')
        else:
            yield events.TextStart(block_id='result')
            yield events.TextDelta(block_id='result', chunk='completed')
            yield events.TextEnd(block_id='result')
            yield events.StreamEnd(finish_reason='stop')


async def work(root: Path, phase: str) -> dict[str, object]:
    store = await SQLiteStore.open(root / 'runs.sqlite')
    operations = ReceiptFixture(root / 'operations.sqlite')
    state_path = root / 'state.json'
    effect = root / 'effect.txt'
    try:
        if phase in {'park', 'park_two'}:
            run, created = await store.begin(
                RunRequest(text='write one line', request_key='process-probe'), {'fixture': True})
            assert created
            await store.set_running(run.id)
            state = {'run_id': run.id}
        else:
            state = json.loads(state_path.read_text())
        ownership = RunOwnership(owner_id='owner', session_id='session', run_id=state['run_id'])
        environment = EnvironmentHandle(owner_id='owner', project_id='project',
                                        environment_id='environment', generation=1, state='ready')

        async def unavailable(args: Args) -> Any:
            raise AssertionError('must use executor pipeline')

        async def execute(invocation: Any, args: Args) -> Any:
            with effect.open('a') as output:
                output.write(f'{args.value}\n')
                output.flush()
                os.fsync(output.fileno())
            return {'value': args.value}

        async def policy(invocation: Any) -> str:
            return 'ask'

        async def approval_request(invocation: Any, remaining: float | None) -> Any:
            return {
                'request_id': 'request-' + invocation.call_id,
                'run_id': invocation.run_id, 'call_id': invocation.call_id,
                'step_index': invocation.step_index,
                'arguments_hash': fingerprint(parse_json(invocation.arguments_json)),
                'tool_hash': invocation.tool_hash,
                'arguments': parse_json(invocation.arguments_json),
                'name': invocation.name, 'expires_at': time.time() + 300,
                'budget_remaining': remaining,
            }

        async def result(ref: str) -> Any:
            snapshot = await store.snapshot(ownership.run_id)
            return next(event.data for event in reversed(snapshot.events)
                        if event.kind == 'tool_end' and event.data['call_id'] == ref)

        schema = {'type': 'object', 'properties': {'value': {'type': 'integer'}},
                  'required': ['value'], 'additionalProperties': False}
        spec = ToolSpec('write', Args, unavailable, schema, '1', 'a' * 64,
                        risk='write', external_executor=execute,
                        source=ToolSource('mcp', 'fixture', 'write', 'b' * 64))
        adapter = SDKAdapter(store=store, operations=operations,
                             executor=ToolExecutor(ToolRegistry((spec,)), policy), tools=[write],
                             environment=environment, ownership=ownership, read_result=result,
                             approval_request=approval_request)
        provider = Scripted(phase)
        if phase == 'replay':
            # The host must not reuse a running checkpoint as another live run.
            before = len(effect.read_text().splitlines())
            try:
                await adapter.run(ai.Model(id='fixture', provider=provider), resume=True,
                                  replies={state['call_id']: True})
            except ServiceError as exc:
                assert exc.code == 'run_closed'
                return {'status': 'replay_rejected', 'effect_lines': before,
                        'model_calls': provider._calls,
                        'receipts': operations.db.execute(
                            'SELECT count(*) FROM receipts').fetchone()[0]}
            raise AssertionError('running checkpoint was replayed')
        if phase not in {'park', 'park_two'}:
            await store.recover()
        if phase == 'stop':
            await store.request_stop(ownership.run_id)
            try:
                await adapter.run(ai.Model(id='fixture', provider=provider), resume=True,
                                  replies={state['call_id']: True})
            except ProbeStopped:
                lines = len(effect.read_text().splitlines()) if effect.exists() else 0
                return {'status': 'stopped', 'model_calls': provider._calls,
                        'effect_lines': lines}
            raise AssertionError('stopped run resumed')
        if phase in {'park', 'park_two'}:
            outcome = await adapter.run(ai.Model(id='fixture', provider=provider),
                                        [ai.user_message('write one line')])
            assert outcome.status == 'awaiting'
            snapshot = await store.snapshot(ownership.run_id)
            state['call_id'] = next(event.data['call_id'] for event in snapshot.events
                                    if event.kind == 'approval_request')
            state_path.write_text(json.dumps(state))
        else:
            outcome = await adapter.run(ai.Model(id='fixture', provider=provider), resume=True,
                                        replies={state['call_id']: phase != 'deny'})
        if outcome.status == 'awaiting' and phase not in {'park', 'park_two'}:
            snapshot = await store.snapshot(ownership.run_id)
            state['call_id'] = next(event.data['call_id'] for event in reversed(snapshot.events)
                                    if event.kind == 'approval_request')
            state_path.write_text(json.dumps(state))
        return {'status': outcome.status, 'model_calls': provider._calls,
                'effect_lines': len(effect.read_text().splitlines()) if effect.exists() else 0,
                'receipts': operations.db.execute('SELECT count(*) FROM receipts').fetchone()[0]}
    finally:
        operations.db.close()
        await store.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    parser.add_argument('phase', choices=['park', 'park_two', 'resume', 'replay', 'stop', 'deny'])
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    print(json.dumps(asyncio.run(work(args.root, args.phase))))
