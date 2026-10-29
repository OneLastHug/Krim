"""新核心的合成夹具；不导入旧管理/存储体系。"""

import asyncio
import errno
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from krim_agent.tools.builtins import EchoInput
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.registry import ToolRegistry


class EventProvider:
    def __init__(self, events):
        self.events, self.calls, self.active, self.streams_closed = events, 0, 0, 0

    async def stream(self, messages, tools):
        self.calls += 1
        self.active += 1
        try:
            for value in self.events:
                await asyncio.sleep(0)
                yield value
        finally:
            self.active -= 1
            self.streams_closed += 1

    async def close(self):
        assert self.active == 0


def registry_for(executor, output=None):
    return ToolRegistry(
        (
            ToolSpec(
                "echo",
                EchoInput,
                executor,
                {"type": "string"} if output is None else output,
                "test-1",
                hashlib.sha256(b"trusted-test-fixture").hexdigest(),
            ),
        )
    )


@pytest.fixture(name="registry_for")
def registry_for_fixture():
    return registry_for


async def pending(service, run_id):
    async with asyncio.timeout(2):
        while True:
            values = await service.approvals(run_id)
            if values:
                return values[0]
            await asyncio.sleep(0.005)


def _entry12_database_path(actual_path):
    return os.path.normpath(os.fspath(actual_path))


@pytest_asyncio.fixture(autouse=True)
async def entry10_resource_observation(request, tmp_path, monkeypatch):
    allowed = {
        "tests/core/test_evaluation.py::test_shared_admission_with_interactive_runs",
        (
            "tests/core/test_evaluation.py::"
            "test_scorer_timeout_and_cancel_join_real_cleanup[cancel]"
        ),
        "tests/core/test_regression.py::test_gate_pass_fail_invalid",
    }
    node_id = request.node.nodeid
    if node_id not in allowed:
        yield
        return

    applications, connections, engine_links, unexpected = [], [], [], []
    capture_errors = []
    listeners = []
    baseline = asyncio.all_tasks()
    fd_before = len(os.listdir("/proc/self/fd"))
    threads_before = [(t.ident, t.name) for t in threading.enumerate()]
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    original = request.module.open_application
    primary = None

    def task_state(task):
        if task is None:
            return None
        return {
            "object": id(task),
            "name": task.get_name(),
            "done": task.done(),
            "cancelled": task.cancelled(),
            "exception": repr(task.exception())
            if task.done() and not task.cancelled() and task.exception() is not None
            else None,
        }

    def exception_handler(current_loop, context):
        unexpected.append(
            {"message": context.get("message"), "exception": repr(context.get("exception"))}
        )
        if previous_handler is None:
            current_loop.default_exception_handler(context)
        else:
            previous_handler(current_loop, context)

    async def opened(*args, **kwargs):
        application = await original(*args, **kwargs)
        captured = {"application": application}
        applications.append(captured)
        try:
            actual_path = args[0] if args else kwargs["path"]
            if not os.path.isabs(os.fspath(actual_path)):
                raise ValueError("resource_database_path_not_absolute")
            descriptor = os.fstat(application.store.fd)
            captured.update(
                database=_entry12_database_path(actual_path),
                store_object=id(application.store),
                engine_object=id(application.store.engine.sync_engine),
                writer_fd_at_open=application.store.fd,
                writer_identity=[descriptor.st_dev, descriptor.st_ino],
            )
        except Exception as capture_error:
            capture_errors.append(repr(capture_error))
        return application

    def writer_state(captured):
        descriptor = captured.get("writer_fd_at_open")
        identity = captured.get("writer_identity")
        if descriptor is None or identity is None:
            return {"closed": None, "observation": "not_observed"}
        try:
            current = os.fstat(descriptor)
        except OSError as error:
            return {
                "closed": True if error.errno == errno.EBADF else None,
                "observation": "fstat_error",
                "errno": error.errno,
            }
        return {
            "closed": [current.st_dev, current.st_ino] != identity,
            "observation": "descriptor_reused"
            if [current.st_dev, current.st_ino] != identity
            else "same_descriptor_identity",
        }

    def linked(connection):
        engine_links.append(
            {
                "connection": id(connection.connection.dbapi_connection),
                "engine_object": id(connection.engine),
                "at": time.monotonic(),
            }
        )

    try:
        for kind in ["connect", "checkout", "reset", "checkin", "close"]:

            def record(*args, kind=kind):
                connections.append(
                    {
                        "event": kind,
                        "connection": None if args[0] is None else id(args[0]),
                        "at": time.monotonic(),
                    }
                )

            event.listen(Pool, kind, record)
            listeners.append((Pool, kind, record))
        event.listen(Engine, "engine_connect", linked)
        listeners.append((Engine, "engine_connect", linked))
        loop.set_exception_handler(exception_handler)
        monkeypatch.setattr(request.module, "open_application", opened)
        yield
    except BaseException as error:
        primary = error
        raise
    finally:
        audit_errors = list(capture_errors)
        rows = []
        try:
            for captured in applications:
                application = captured["application"]
                evaluation = application.evaluations
                eval_kernel = None if evaluation is None else getattr(evaluation, "kernel", None)
                rows.append(
                    {k: v for k, v in captured.items() if k != "application"}
                    | {
                        "application_object": id(application),
                        "application_close_task": task_state(application._close_task),
                        "store_closed": application.store.closed,
                        "writer_fd_after": application.store.fd,
                        "writer_descriptor": writer_state(captured),
                        "run_service_closed": application.service.closed,
                        "run_close_task": task_state(application.service._close_task),
                        "run_tasks": {
                            key: task_state(task) for key, task in application.service.tasks.items()
                        },
                        "eval_kernel_observed": eval_kernel is not None,
                        "eval_kernel_closed": None if eval_kernel is None else eval_kernel.closed,
                        "eval_close_task": None
                        if eval_kernel is None
                        else task_state(eval_kernel._close_task),
                        "eval_tasks": None
                        if eval_kernel is None
                        else {key: task_state(task) for key, task in eval_kernel.tasks.items()},
                        "provider_calls": getattr(application.provider, "calls", None),
                        "provider_active": getattr(application.provider, "active", None),
                        "provider_streams_closed": getattr(
                            application.provider, "streams_closed", None
                        ),
                    }
                )
            pending_tasks = [
                task_state(task)
                for task in asyncio.all_tasks()
                if task not in baseline and task is not asyncio.current_task() and not task.done()
            ]
            receipt = {
                "node": node_id,
                "uid": os.getuid(),
                "consumer": request.module.__file__,
                "applications": rows,
                "connections": connections,
                "engine_links": engine_links,
                "unexpected": unexpected,
                "pending": pending_tasks,
                "fd_before": fd_before,
                "fd_after": len(os.listdir("/proc/self/fd")),
                "threads_before": threads_before,
                "threads_after": [(t.ident, t.name) for t in threading.enumerate()],
                "borrowed_executor_threads_not_claimed_zero": True,
                "active_product_cleanup_by_observer": False,
                "primary_error": None
                if primary is None
                else {"type": type(primary).__name__, "message": str(primary)},
            }
            if not rows or unexpected or pending_tasks:
                audit_errors.append("application_missing_or_pending_or_unexpected")
            for row in rows:
                close = row["application_close_task"]
                if (
                    close is None
                    or not close["done"]
                    or close["cancelled"]
                    or close["exception"] is not None
                    or not row["store_closed"]
                    or row["writer_descriptor"]["closed"] is not True
                    or not row["run_service_closed"]
                    or any(not task["done"] for task in row["run_tasks"].values())
                    or not row["eval_kernel_observed"]
                    or not row["eval_kernel_closed"]
                    or any(not task["done"] for task in (row["eval_tasks"] or {}).values())
                ):
                    audit_errors.append("natural_close_not_confirmed:" + str(row.get("database")))
            states = {}
            for recorded in connections:
                identity, kind = recorded["connection"], recorded["event"]
                expected = {
                    "connect": "closed",
                    "checkout": "idle",
                    "reset": "out",
                    "checkin": "reset",
                    "close": "idle",
                }[kind]
                if identity is None or states.get(identity, "closed") != expected:
                    audit_errors.append("connection_lifetime:" + repr(recorded))
                states[identity] = {
                    "connect": "idle",
                    "checkout": "out",
                    "reset": "reset",
                    "checkin": "idle",
                    "close": "closed",
                }[kind]
            receipt["connection_end_states"] = states
            if any(value != "closed" for value in states.values()):
                audit_errors.append("connection_not_closed")
            if receipt["fd_after"] > fd_before:
                audit_errors.append("fd_increase")
        except Exception as audit_error:
            audit_errors.append(repr(audit_error))
            receipt = {
                "node": node_id,
                "uid": os.getuid(),
                "applications": rows,
                "connections": connections,
                "engine_links": engine_links,
                "unexpected": unexpected,
            }
        finally:
            for target, kind, listener in reversed(listeners):
                try:
                    event.remove(target, kind, listener)
                except Exception as remove_error:
                    audit_errors.append("listener_restore:" + repr(remove_error))
            try:
                if request.module.open_application is opened:
                    request.module.open_application = original
            except Exception as restore_error:
                audit_errors.append("wrapper_restore:" + repr(restore_error))
            finally:
                loop.set_exception_handler(previous_handler)
        receipt["audit_errors"] = audit_errors
        try:
            await asyncio.to_thread(
                (tmp_path / "entry10-agent-resources.json").write_text,
                json.dumps(receipt, indent=2),
            )
        except Exception as publication_error:
            audit_errors.append("receipt_publication:" + repr(publication_error))
        if audit_errors:
            if primary is not None:
                primary.add_note("entry10 resource observation: " + repr(audit_errors))
            else:
                raise AssertionError(audit_errors)
