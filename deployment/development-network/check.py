"""Run B2's ordinary-user checks and pin source hashes beside raw output."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    sources = sorted((root / 'src/krim_agent/dev_network').glob('*.py')) + [
        root / 'src/krim_agent/dev_environment' / name
        for name in ('backend.py', 'docker.py', 'execution.py', 'model.py')
    ]
    tests = sorted((root / 'tests/agent_core').glob('test_dev_network_*.py')) + [
        root / 'tests/agent_core' / name for name in (
            'test_dev_environment_backend.py', 'test_dev_environment_execution.py',
            'test_dev_environment_fencing.py', 'test_dev_environment_review.py',
        )
    ]
    def hashes() -> dict[str, str]:
        return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in [*sources, *tests, Path(__file__)]}

    before = hashes()
    env = dict(os.environ, PYTHONPATH=str(root / 'src'), PYTHONDONTWRITEBYTECODE='1')
    commands = {
        'pytest': [sys.executable, '-m', 'pytest', '-q', *map(str, tests)],
        'ruff': [sys.executable, '-m', 'ruff', 'check', *map(str, [*sources, *tests]),
                 str(root / 'deployment/development-network')],
        'mypy': [sys.executable, '-m', 'mypy', *map(str, sources)],
    }
    results = {}
    for label, command in commands.items():
        result = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True)
        (evidence / f'{label}.txt').write_text(result.stdout + result.stderr)
        results[label] = {'command': command, 'exit': result.returncode,
                          'output': result.stdout + result.stderr}
        print(json.dumps({'check': label, 'exit': result.returncode}), flush=True)
    after = hashes()
    status = 0 if before == after and all(r['exit'] == 0 for r in results.values()) else 1
    (evidence / 'checks.json').write_text(json.dumps({
        'uid': os.getuid(), 'interpreter': sys.executable, 'results': results,
        'source_hashes_before': before, 'source_hashes_after': after,
        'source_unchanged': before == after, 'exit': status,
    }, indent=2))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
