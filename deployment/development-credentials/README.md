# Project Secret delivery and native Git authentication

This package adds host-only adapters to the existing project-access domain. It uses `AuthenticatedSecretVault` / `MasterKeyring` from account MCP, the existing SQLite access repository, Git CLI, and Git's native credential-cache. No new crypto, database, Git engine or dependency is introduced.

## Host assembly

I supplies the existing trusted keyring/vault and `SQLiteAccessRepository`. Do not expose these adapters as arbitrary Python callbacks, helper strings, filesystem paths, shell environment variables or model tools.

```python
from krim_agent.project_access.delivery import SealedDeliveryQueue
from krim_agent.project_access.secrets import DeliveryBinding
from krim_agent.project_access.service import ProjectAccessService

pending = SealedDeliveryQueue(vault, capacity=16, max_bytes=16384, ttl=10)
access = ProjectAccessService(
    repository, vault, secret_sink=pending, git_sink=pending,
    delivery_sink=trusted_nonblocking_sink,
)

# Authenticated host ingress builds exact intent/ref and persists SDK→D mapping.
receipt = await access.deliver_secret(ref, intent)
secret_id, revision = receipt.secret_revisions[0]
binding = DeliveryBinding(intent, ref, secret_id, revision)
await access.release_secret(binding, pending)
```

`release_secret(binding, pending)` uses the constructor-wired `delivery_sink` and reacquires the current SQLite guard, checks actor generation, project grant, approval, scope, expiry, secret version and purpose, and consumes one encrypted queue entry. The sink must accept synchronously into a bounded pipe/queue; it must never await a network or process while holding the transaction. Ciphertext remains pending only until consumption, TTL eviction, project revoke, or close. Process restart drops the in-memory queue. I owns the durable operation record; queue acceptance is not operation admission.

### Git credential channel

`NativeGitCredentialCache(root, binding, git_executable=Path('/usr/bin/git'), timeout=30)` is a synchronous SecretSink for one clone/push operation. Construct it outside the SQLite guard. The constructor starts a waiting native helper. `deliver` performs one nonblocking atomic stdin write; token length is also bounded by platform PIPE_BUF. `await ready()` waits outside the guard. `helper` is a nonsecret native credential-helper argument; `binding` is available only after successful preparation. Always `close()` in finally, including failure, cancellation and committed revocation.

```python
from krim_agent.project_access.git_credentials import NativeGitCredentialCache, helper_request

current = await access.resolve(ref, intent)
secret_id, revision = current.secret_revisions[0]
binding = DeliveryBinding(intent, ref, secret_id, revision)
cache = NativeGitCredentialCache(private_root, binding)
try:
    # This service instance shares the same authoritative repository/vault.
    git_access = ProjectAccessService(
        repository, vault, secret_sink=pending, git_sink=cache,
    )
    await git_access.deliver_git_credential(
        ref, intent, helper_request(request.git), helper_action='get',
    )
    await cache.ready()
    result = await runner.execute(
        intent, ref, request, git_access,
        credential_helper=(cache.helper,), credential_binding=cache.binding,
        transport=git_network_transport,
    )
finally:
    cache.close()
```

The initial `resolve` is informational: rotation before helper delivery or actual Git acceptance makes the stale binding fail. Rebuild the host operation resource after a fresh authoritative resolution; never silently change the approved intent.

The helper's socket directory is unique per operation, mode 0700 and owner checked. Its trusted parent and executable must be provisioned by I. They must be inaccessible to ordinary Shell/project containers; a 0700 directory alone does not isolate untrusted processes running as the same OS user. Do not mount this root or Git metadata into those containers. Token bytes only cross stdin and Git's private cache socket. They do not enter argv, environment, project files, model output or normal logs. Cache TTL bounds orphan lifetime after an abrupt host crash; it does not replace active revoke cleanup.

### Actual Git acceptance

`ProjectAccessService.accept_git(ref, intent, accept, credential_binding=None)` rechecks under `AccessRepository.guard`, then invokes a trusted bounded synchronous spawn callback with the current `GitIdentity`. A configured private Git grant requires a credential binding for clone/push. The service checks the exact intent/ref and current secret revision, Git credential id/purpose, and repository's network policy membership. Waiting for Git is outside the guard.

`TrustedGitRunner` accepts only typed `GitExecutionRequest`; all request fields are included in `intent.arguments_json`. It maps the project/repository into its own private metadata directory. It builds argv from the typed action, uses exact refs/OIDs and CAS, resets the helper chain/configuration/hooks, and returns structured results without stdout/stderr. It supports clone, diff, commit and push through native Git. An operation lock prevents the runner from changing the same local repository concurrently. Push first checks the advertised exact ref/OID, then uses Git's force-with-lease at receive-pack; the precheck also rejects stale expectations for no-op pushes. Only this adapter's canonical native cache helper is allowed. I persists operation admission to prevent replay across hosts/restarts.

The bare metadata root contains host-staged objects. I must bridge approved workspace changes into that private object store before a commit request names its tree; ordinary Shell never receives access to Git credentials or metadata. `runner.invalidate(owner_id, project_id)` kills active process groups for that exact scope. Timeout and cancellation also kill and reap accepted processes, and failed clone cleanup verifies the newly created directory's device/inode before deletion.

`GitTransport.spawn(operation, argv, *, cwd, env, stdin, stdout, stderr)` is host-only. I must supply an isolated and enforced network lease for the exact repository/operation; its teardown must kill the accepted process and close the credential cache. No transport means HTTPS clone/push fail closed. B2's GET/HEAD download broker does not support Git push POST. There is no fallback to ordinary networked Shell.

## Lifecycle integration owned by I

- Authenticate caller and derive ref actor/generation; neither an access receipt nor a delivery binding authenticates callers.
- Reuse the existing master-key provider; D adapters never locate or read key files. Provision/rotate `SecretRecord` with existing vault and monotonically increasing revisions through repository CAS.
- Persist SDK operation identity → D intent digest → typed Git request → actual process/lease binding. SDK, D and command digests have different domains.
- On committed grant/secret/actor/network revocation, invalidate pending project queue entries and cancel the exact accepted Git process/lease/cache under host management authority. Do not require the revoked actor to authorize cleanup.
- Bound process count, runtime and private disk usage at the host level; collect exact operation resources after completion.

## Verification scope

Tests use generated fixture keyrings, real AES-GCM, real SQLite transactions, Git's native Unix-socket credential helper, and temporary local bare remotes. No real provider credentials, internet repository or container network isolation is exercised. No package installation, account configuration, real push or UI change is required. Remote provider authentication remains gated on I's dedicated Git network transport and lifecycle assembly.
