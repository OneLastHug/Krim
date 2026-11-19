# Krim personal sandbox node runtime image (normal jobs / installs / connectors).
#
# Build contract (owned by the integration builder, not by package C1):
#   --build-arg BASE_IMAGE=<repo>@sha256:<64 hex>
# BASE_IMAGE is REQUIRED and has no default: an undigested base fails the build.
# Until the builder builds and verifies a digest, the manifest entry stays
# availability=not_built with image=null and RuntimeCatalog.resolve refuses it.
# This file contains no network steps; only the base pull is the builder's.

ARG BASE_IMAGE

FROM ${BASE_IMAGE}

ARG BASE_IMAGE

# Fail closed on any base reference that is not repo@sha256.
RUN case "${BASE_IMAGE}" in \
        *@sha256:*) ;; \
        *) printf 'BASE_IMAGE must be repo@sha256:<64 hex>, got: %s\n' "${BASE_IMAGE}" >&2; exit 1 ;; \
    esac

LABEL org.opencontainers.image.title="krim-sandbox-node" \
      org.opencontainers.image.description="Krim personal sandbox node runtime; base digest recorded for release verification" \
      krim.runtime="node" \
      krim.interpreter="/usr/local/bin/node" \
      krim.run_as="65532:65532" \
      krim.base_image="${BASE_IMAGE}"

# Interpreter path must match the profile contract (/usr/local/bin/node) on the
# official node base image. Hardening flags are passed per run by the profile
# (--disallow-code-generation-from-strings, --max-old-space-size), never baked
# into the image, so the manifest stays the single source of truth.
RUN if ! [ -x /usr/local/bin/node ]; then \
        printf 'expected /usr/local/bin/node on the base image\n' >&2; exit 1; \
    fi \
    && groupadd --gid 65532 krim \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin krim \
    && mkdir --mode=0755 /workspace \
    && chown 65532:65532 /workspace

# Read-only-rootfs compatible: HOME on the runtime tmpfs, NODE_OPTIONS pinned
# empty so environment injection cannot smuggle extra runtime flags. Writable
# paths at run time are exactly /workspace (bind mount) and /tmp (tmpfs with
# nosuid,nodev,noexec); nothing else is writable.
ENV NODE_OPTIONS="" \
    HOME=/tmp

USER 65532:65532
WORKDIR /workspace
STOPSIGNAL SIGTERM
