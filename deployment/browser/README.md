# Read-only browser foundation

The first supported command set is `navigate`, `snapshot`, `screenshot`, and
`close`. This repository currently contains only a fail-closed host protocol and
exact-origin egress policy; it does **not** contain or launch a browser worker.

A production worker must be a separately built, digest-pinned, non-root image
with one ephemeral context/page per run. It must have no direct network route:
all DNS and outbound traffic must pass through a dedicated policy proxy. The
proxy must enforce exact HTTPS origins/ports, inspect every redirect, subresource
and WebSocket destination, resolve through an approved DNS path, reject any
private or mixed DNS answers, and pin approved addresses. The proxy must connect,
resolve, and set TLS SNI using only the canonical `(host, port)` returned by the
policy check; it must never reuse the raw URL authority after authorization. Browser-side request
interception must restrict methods to GET/HEAD/OPTIONS; a CONNECT-only proxy
cannot enforce methods inside TLS. QUIC, WebRTC, UDP, proxy bypass, popups and
persistent profiles must be disabled or blocked at the network boundary.

Never add Chromium to the command sandbox or relax `runsc` network=`none`.
Do not expose CDP, a worker WebSocket, arbitrary JavaScript, click/fill, login,
upload or download operations. Keep screenshot bytes bounded and validate PNG
content before publishing them through an authenticated workspace ArtifactStore
revision. A dedicated public artifact-publish operation and run/session-bound
worker lifecycle integration remain required before screenshots can be returned
as artifact references.

Before deploying a concrete worker, verify image digest, non-root UID, resource
limits, process-tree cleanup, one-context-per-run isolation, proxy reachability,
no direct sockets, DNS rebinding/mixed-answer rejection, redirect/subresource/
WebSocket enforcement, method filtering, timeout/cancellation cleanup, and no
replay after worker loss. The host protocol alone does not prove those properties.
