# Krim personal sandbox python runtime image (normal jobs / installs / connectors).
#
# Build contract (owned by the integration builder, not by package C1):
#   --build-arg BASE_IMAGE=<repo>@sha256:<64 hex>
# BASE_IMAGE is REQUIRED and has no default: an undigested base fails the build.
# The builder verifies the digest against the release record before publishing
# it in runtime_images/runtime-manifest.json. Tags and "latest" are rejected
# by the guard below and never appear in this file. This image is NOT built in
# the C1 round: apt steps need network and are executed centrally by the
# builder when authorized.

ARG BASE_IMAGE

FROM ${BASE_IMAGE}

ARG BASE_IMAGE

# Fail closed on any base reference that is not repo@sha256.
RUN case "${BASE_IMAGE}" in \
        *@sha256:*) ;; \
        *) printf 'BASE_IMAGE must be repo@sha256:<64 hex>, got: %s\n' "${BASE_IMAGE}" >&2; exit 1 ;; \
    esac

LABEL org.opencontainers.image.title="krim-sandbox-python" \
      org.opencontainers.image.description="Krim personal sandbox python runtime; base digest recorded for release verification" \
      krim.runtime="python" \
      krim.interpreter="/usr/bin/python3.12" \
      krim.run_as="65532:65532" \
      krim.base_image="${BASE_IMAGE}"

# Interpreter path must match the profile contract (/usr/bin/python3.12):
# build on a base that ships it (e.g. ubuntu noble), never relocate the binary.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes python3.12 \
    && rm -rf /var/lib/apt/lists/* \
    && /usr/bin/python3.12 -I -c "import sys; assert sys.version_info[:2] == (3, 12), sys.version" \
    && groupadd --gid 65532 krim \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin krim \
    && mkdir --mode=0755 /workspace \
    && chown 65532:65532 /workspace

# Read-only-rootfs compatible: no bytecode writes, unbuffered streams, HOME on
# the runtime tmpfs. Writable paths at run time are exactly /workspace (bind
# mount) and /tmp (tmpfs with nosuid,nodev,noexec); nothing else is writable.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp

USER 65532:65532
WORKDIR /workspace
# RunscRunner stops containers with SIGTERM; the trusted supervisor forwards it
# to the payload process group before the hard deadline.
STOPSIGNAL SIGTERM
