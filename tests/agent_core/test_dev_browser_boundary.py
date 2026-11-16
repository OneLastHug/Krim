"""Final boundary cases that exercise output withholding and E redirects."""

import pytest
from test_dev_browser_session import ORIGIN, png, session

from krim_agent.dev_browser import BrowserError
from krim_agent.dev_browser.policy import relative_target
from krim_agent.dev_preview.boundary import response_headers
from krim_agent.dev_preview.service import PreviewError


@pytest.mark.parametrize("path", ["//[bad", "/%2525252fother", "/%2525252ekrim-preview/session"])
def test_invalid_authority_and_deep_encoding_return_fixed_errors(path):
    with pytest.raises(BrowserError, match="^browser_target_invalid$"):
        relative_target(path)


@pytest.mark.asyncio
async def test_revocation_after_capture_withholds_artifact():
    context, check, adapter = await session()

    async def capture(**kwargs):
        check.revoked = True
        return png()

    context.page.screenshot = capture
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.screenshot()
    assert context.closed.is_set()


@pytest.mark.parametrize(
    "location",
    [
        b"https://other.preview.example/file",
        b"http://127.0.0.1/private",
        b"//external.test/private",
        b"/\\external.test/private",
    ],
)
def test_existing_e_boundary_rejects_cross_origin_redirects(location):
    with pytest.raises(PreviewError, match="preview_redirect_denied"):
        response_headers(
            [(b"location", location)],
            origin=ORIGIN,
            upstream="http://p-fixed.preview.example",
            control_origin="https://krim.example.com",
        )
