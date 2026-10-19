"""AC-M1独立合成夹具；仅导入本轮发行源码。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest_asyncio
from fixtures.session_server import WireFixture


@pytest_asyncio.fixture
async def wire():
    value = await WireFixture().start()
    try:
        yield value
    finally:
        await value.close()
