# Isolated Git transport and workspace import

This increment adds a concrete Docker/runsc Git transport and a workspace-to-tree adapter. The previously accepted D2 files remain byte-for-byte unchanged. Native Git performs clone, negotiation, object creation, commit and receive-pack lease checks; the bridge does not implement the Git protocol.

## Deployment

Use the existing trusted Linux host management process that already owns the fixed Docker socket. No Docker permission change, image pull, package install, host networking, privileged container, new capability, kernel setting or listener on the host is required. The approved image used for verification is `sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc`; it contains `/usr/bin/git` 2.39.5 and `/usr/bin/python3` 3.11.2. Pass the host interpreter with the existing project dependencies.

Provision private `repository_root`, `stage_root` and credential-cache roots on the F-backed Krim WSL native filesystem, with host owner and mode 0700. Do not use a `/mnt/f` DrvFS bind mount for container Git metadata: the approved Git/runsc combination exited 139 during `git init` there. The same image and isolation succeeded on the WSL native filesystem. This is a path deployment requirement, not a change to Docker/kernel/network configuration. Source workspaces may remain in their existing trusted locations.

Each operation creates its own container with `--runtime=runsc`, `--network=none`, user `65532:65532`, `--cap-drop=ALL`, `--security-opt=no-new-privileges:true`, read-only root filesystem, 256 MiB memory, 0.5 CPU, 64 PIDs and bounded tmpfs. Only the operation's private metadata stage is mounted. No workspace, credential directory or Docker socket is mounted. Container lifetime is at most 120 seconds; each HTTP body is limited to 16 MiB and the operation transfer total to 64 MiB. The staged existing repository is limited to 128 MiB and 65,536 entries. Larger operations fail and need a separately reviewed configuration change.

## Trust and operation binding

The container runs only the fixed native Git invocation and fixed loopback HTTP shim. It is not a development environment, and its ID/exec interface must never be registered with ordinary Shell tools. Native Git executes no hooks, inherited configuration, filters, submodule recursion or credential helper. The host cache's credential goes only to the exact repository's HTTPS request header; the container receives neither it nor a reusable proxy capability.

`spawn` matches the complete generated command suffix, repository, ref, action, OID, expected-old OID and working directory. A clone and each push preflight/push command are single-use. Push uses native `--force-with-lease=exact-ref:expected-old`; Git's server performs the update CAS. The existing runner's exact-ref preflight remains in place. A clone stays in its private stage until native Git verifies the authorized ref/OID and that no other ref was imported; promotion checks the target's original device/inode.

The loopback shim and host HTTP broker allow only the operation's repository `info/refs`, `git-upload-pack` or `git-receive-pack` endpoints. They use stdlib HTTP parsing and HTTPX/PinnedAsyncTransport for upstream DNS/IP/TLS checks. There is no CONNECT tunnel, redirect following or client-supplied Authorization/cookie. Request upload chunks and response chunks re-resolve the original authority; changed grants/revisions are rejected. The broker does not validate arbitrary receive-pack payloads independently: exact ref/OID enforcement relies on the exclusively trusted native Git process. Never admit unrelated code into this container.

## Host API

```python
from krim_agent.project_access.git_transport import (
    DockerGitTransport, GitNetworkAuthorization, GitTransportBinding,
)

async def resolve_current():
    # The closure captures original authenticated ref/intent and the complete
    # current environment record, not caller-supplied identifiers.
    access_receipt = await access.resolve(ref, intent)
    policy = await access.network_policy(ref, intent)
    record = await environments.resolve(environment)
    return GitNetworkAuthorization(
        access_receipt, policy, record.environment, record.image_id,
    )

lease = DockerGitTransport(
    GitTransportBinding(environment, ref, intent, record.image_id),
    stage_root=trusted_stage_root,
    repository_root=runner.root,
    resolve=resolve_current,
    cache=cache,  # NativeGitCredentialCache already prepared through D service
)
try:
    # Host derives this exact owner/project/repository path from runner.root.
    # For clone, it must not exist yet; the runner reserves it at acceptance.
    await lease.prepare(private_repository_path)
    result = await runner.execute(
        intent, ref, request, access, transport=lease,
        credential_helper=(cache.helper,), credential_binding=cache.binding,
    )
finally:
    await lease.close()
```

For a public grant use `cache=None` and omit the helper/binding arguments. The authoritative projection must have no secret revisions in this case. `repository_root` is constructor-fixed and checked with its device/inode; an alternate store with identical final directory names is rejected.

I registers the lease before preparation and broadcasts `await lease.close()` at the committed revoke boundary, alongside `runner.invalidate(owner_id, project_id)`. Close permanently blocks new requests, clears host authentication, closes the cache, verifies the exact created container's binding, removes it and reaps worker/bridge processes. A failed cleanup remains retryable. Prepare/close are fenced so a pending resolver or Docker create cannot publish a lease after close completes. The 0.1-second watcher and 120-second lifetime are fallbacks, not substitutes for committed-revocation notification across workers.

I still owns caller authentication, approval, durable operation deduplication, actor/environment mapping and recovery records. The resolver should read those current facts consistently; D rechecks the entire returned projection but cannot make independently fetched host records transactional.

## Workspace objects

```python
from krim_agent.project_access.git_workspace import GitWorkspaceBridge, WorkspaceBinding

source = WorkspaceBinding(
    record.environment, record.content_path,
    record.content_device, record.content_inode,
)
async def verify_source(current, destination):
    # I re-resolves the complete environment/root binding and authenticates
    # this destination against owner/project/canonical repository mapping.
    await verify_current_environment_and_destination(current, destination)

tree = await GitWorkspaceBridge().import_tree(
    source, private_repository_path, verify=verify_source,
)
# The host then builds/approves a typed git.commit request whose tree_id is
# tree.tree_id and expected_old_id is the approved parent/ref CAS value.
```

The bridge opens the trusted source with directory descriptors and O_NOFOLLOW, rejects symlinks/hardlinks/special files, bounds file count, bytes and depth, and checks file/directory metadata again before returning. `.git` metadata is excluded; project config/hooks/filters are never executed. It uses Git `hash-object --no-filters`, an empty private index, `update-index --index-info` and `write-tree`. Defaults are 4,096 files, 32 MiB total, 8 MiB per file and depth 32. The temporary index is beside the trusted destination and is removed on success/failure. Verify is called before, during and after bounded work and receives both source binding and destination. No ref is updated by import itself.

## Verification

`verify_increment.py --output <evidence-directory>` runs the existing D2 tests plus increment tests, Ruff and mypy, records hashes, and checks the original 15-file manifest when available. It does not start containers. `verify_transport_live.py` uses the existing image, F-backed WSL native temporary directories, fixture credentials and a local native `git http-backend`. It verifies container clone, workspace import/push, direct TCP denial, absence of credential files, generation revocation and exact container/cache cleanup. Upstream HTTPS sockets and authorization are test substitutes in that script; no real provider account or external Git repository is contacted.

The increment introduces four package modules (`git_transport`, `git_transport_shim`, `git_transport_worker`, `git_workspace`). I must include them in explicit wheel packaging if the package uses an allowlist, and supply the paths/resolver/lifecycle wiring above. No dependency lock or shared schema change is required.
