"""Opt-in real resize check: fixed fulfilled page, no server/TLS/preview claim."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path

import pytest

IMAGE = os.environ.get("F_RESIZE_IMAGE_ID", "")
PROFILE = Path("/opt/krim-development-browser/v1.61.0/seccomp-chroot.json")
PROFILE_SHA = "f922c7c9bfc1ece0b72b244e56dc85756ded002c837fb46054cfd8b275c74b6b"
DOCKER = ["/usr/bin/docker", "--host=unix:///var/run/docker.sock"]
PROBE = r'''
import asyncio, hashlib, json, os, struct, sys
sys.path.insert(0, "/opt/krim-browser/app")
from playwright.async_api import async_playwright
from krim_agent.dev_browser import BrowserSession
from worker import dispatch

ORIGIN = "https://p-resize-fixture.preview.example"
HTML = """<!doctype html><meta name="viewport" content="width=device-width, initial-scale=1">
<style>#layout::after{content:'desktop'}@media(max-width:400px){#layout::after{content:'mobile'}}
body{margin:0;font-family:sans-serif}#layout{padding:12px;background:#eee}</style>
<div id="layout">Fixed resize component: </div>"""

async def main():
    assert (os.getuid(), os.getgid()) == (1000, 1000)
    assert sorted(os.listdir('/sys/class/net')) == ['lo']
    checks = 0
    async def authority_fixture():
        nonlocal checks
        checks += 1
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, chromium_sandbox=True)
        context = await browser.new_context(
            accept_downloads=False, service_workers='block', ignore_https_errors=False,
            viewport={'width':1024,'height':768}, device_scale_factor=1,
        )
        adapter = BrowserSession(context, ORIGIN, authority_fixture)
        try:
            await adapter.start()
            page = context.pages[0]
            requests = []
            async def fixture(route):
                requests.append(route.request.url)
                await route.fulfill(status=200,content_type='text/html',body=HTML)
            # This local page route supplies bytes without dialing any server.
            # It is an explicit fixture, not evidence of preview authorization.
            await page.route(ORIGIN+'/', fixture)
            await adapter.goto('/')
            snapshots = []
            for preset, size, marker in (
                ('mobile',(375,812),'mobile'), ('desktop',(1024,768),'desktop'),
            ):
                response = await dispatch(adapter, {'op':'resize','preset':preset})
                assert response == {'done':True}
                assert page.viewport_size == {'width':size[0],'height':size[1]}
                actual = await page.locator('#layout').evaluate(
                    "element => getComputedStyle(element, '::after').content"
                )
                assert actual == '"'+marker+'"'
                artifact = await adapter.screenshot()
                assert struct.unpack('>II',artifact.data[16:24]) == size
                snapshots.append({'preset':preset,'width':size[0],'height':size[1],
                    'layout':marker,'size':len(artifact.data),
                    'sha256':hashlib.sha256(artifact.data).hexdigest()})
            assert requests == [ORIGIN+'/']  # resize does not reload
            print(json.dumps({'ok':True,'chromium':browser.version,'snapshots':snapshots,
                'checks':checks,'actions':adapter.diagnostics()['actions'],
                'fixture_requests':len(requests),'preview_authorization_tested':False}),flush=True)
        finally:
            await adapter.close()
            await browser.close()
asyncio.run(main())
'''


@pytest.mark.skipif(
    not IMAGE, reason="requires approved local image and Docker management entrypoint"
)
def test_fixed_resize_presets_in_real_sandboxed_browser():
    assert os.getuid() == 0 and re.fullmatch(r"sha256:[a-f0-9]{64}", IMAGE)
    assert hashlib.sha256(PROFILE.read_bytes()).hexdigest() == PROFILE_SHA
    evidence = Path(os.environ["F_RESIZE_EVIDENCE"])
    assert evidence.is_absolute() and not evidence.is_relative_to("/mnt") and not evidence.exists()
    evidence.mkdir(mode=0o700, parents=True)
    name = "krim-browser-resize-" + uuid.uuid4().hex
    cid = None
    record = {"image": IMAGE, "profile_sha256": PROFILE_SHA, "removed": False}

    def docker(*args, seconds=15):
        return subprocess.run(
            [*DOCKER, *args], capture_output=True, text=True, timeout=seconds, check=False
        )

    try:
        created = docker(
            "create",
            "--name=" + name,
            "--pull=never",
            "--runtime=runc",
            "--user=1000:1000",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--security-opt=seccomp=" + str(PROFILE),
            "--network=none",
            "--read-only",
            "--init",
            "--restart=no",
            "--log-driver=none",
            "--memory=1073741824",
            "--memory-swap=1073741824",
            "--cpus=2",
            "--pids-limit=128",
            "--shm-size=134217728",
            "--ipc=private",
            "--ulimit=nofile=256:256",
            "--tmpfs=/tmp:rw,nosuid,nodev,size=134217728,mode=1777",
            "--tmpfs=/tmp/krim-home:rw,nosuid,nodev,size=33554432,uid=1000,gid=1000,mode=700",
            "--env=HOME=/tmp/krim-home",
            "--env=NODE_EXTRA_CA_CERTS=",
            "--entrypoint=/opt/krim-browser/venv/bin/python",
            IMAGE,
            "-I",
            "-c",
            PROBE,
        )
        assert created.returncode == 0, created.stderr
        cid = created.stdout.strip()
        assert re.fullmatch(r"[a-f0-9]{64}", cid)
        record["container_id"] = cid
        before = json.loads(docker("inspect", cid).stdout)[0]
        assert before["Image"] == IMAGE and before["Name"] == "/" + name
        host = before["HostConfig"]
        assert host["NetworkMode"] == "none" and host["CapDrop"] == ["ALL"]
        assert host["ReadonlyRootfs"] and not host.get("Privileged") and not host.get("CapAdd")
        record["limits"] = {
            k: host[k]
            for k in (
                "NetworkMode",
                "ReadonlyRootfs",
                "CapDrop",
                "Memory",
                "MemorySwap",
                "NanoCpus",
                "PidsLimit",
            )
        }
        started = time.monotonic()
        result = docker("start", "--attach", cid, seconds=90)
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (evidence / "stdout.txt").write_text(result.stdout)
        (evidence / "stderr.txt").write_text(result.stderr[:8192])
        assert result.returncode == 0, result.stderr
        record["probe"] = json.loads(result.stdout)
        assert record["probe"]["ok"]
    finally:
        if cid is not None:
            found = docker("inspect", cid)
            if found.returncode == 0:
                current = json.loads(found.stdout)[0]
                assert current["Image"] == IMAGE and current["Name"] == "/" + name
                removed = docker("rm", "--force", cid)
                record["removed"] = (
                    removed.returncode == 0 and docker("inspect", cid).returncode != 0
                )
        (evidence / "result.json").write_text(json.dumps(record, indent=2) + "\n")
        assert cid is None or record["removed"], "exact container cleanup unconfirmed"
