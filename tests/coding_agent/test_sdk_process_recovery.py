"""Cross-process SDK checkpoints with real SQLite and a filesystem side effect."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

WORKER = Path(__file__).with_name('sdk_process_worker.py')


def step(root: Path, phase: str) -> dict[str, object]:
    result = subprocess.run([sys.executable, str(WORKER), str(root), phase],
                            capture_output=True, text=True, timeout=30, env=os.environ.copy())
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def test_approval_and_receipt_survive_new_process_without_repeating_effect(tmp_path: Path) -> None:
    parked = step(tmp_path, 'park')
    assert parked['status'] == 'awaiting'
    assert parked['effect_lines'] == parked['receipts'] == 0
    resumed = step(tmp_path, 'resume')
    assert resumed['status'] == 'completed'
    assert resumed['effect_lines'] == resumed['receipts'] == 1
    assert resumed['model_calls'] == 1
    replayed = step(tmp_path, 'replay')
    assert replayed['status'] == 'replay_rejected'
    assert replayed['effect_lines'] == replayed['receipts'] == 1
    assert replayed['model_calls'] == 0


def test_same_batch_completed_write_is_not_repeated_after_second_approval(tmp_path: Path) -> None:
    parked = step(tmp_path, 'park_two')
    assert parked['effect_lines'] == 0
    first = step(tmp_path, 'resume')
    assert first['status'] == 'awaiting'
    assert first['effect_lines'] == first['receipts'] == 1
    second = step(tmp_path, 'resume')
    assert second['status'] == 'completed'
    assert second['effect_lines'] == second['receipts'] == 2
    assert (tmp_path / 'effect.txt').read_text().splitlines() == ['7', '8']


@pytest.mark.parametrize('phase', ['stop', 'deny'])
def test_stop_or_denial_after_process_restart_never_writes(tmp_path: Path, phase: str) -> None:
    step(tmp_path, 'park')
    result = step(tmp_path, phase)
    assert result['effect_lines'] == 0
    if phase == 'stop':
        assert result['status'] == 'stopped'
        assert result['model_calls'] == 0
    else:
        assert result['status'] == 'completed'
        assert result['receipts'] == 0
