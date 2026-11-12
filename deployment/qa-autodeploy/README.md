# QA autodeploy runtime checks

Host-side deployment source, kept separate from the application runtime. Installing this file requires the operator's explicit service-deployment scope. It is not imported by Krim and does not change main/QA branch policy.

## Invariants

- Build/test environments are not the production release. Create the final virtualenv, install the application wheel **with dependencies**, preserve the deployment's MCP v1 support, run `python -m pip check`, and import the actual CLI in that final environment.
- Before switching, start the final runtime against consistent SQLite backups, under the QA unit's hardening and credentials with external network access disabled. Startup must be healthy and both schema versions must remain unchanged. Schema upgrades require a separately reviewed deployment rather than an automatic retry.
- Build or preflight failure must not change the serving drop-in or restart the existing service.
- Failed cutover restores the exact previous auto drop-in bytes, rather than removing all auto drop-ins and falling back to an obsolete base unit. Report rollback health separately; never treat a successful restart command as recovery.
- Never restore database backups automatically, lower schema metadata, or reset user data.

The script is deployment-specific; review its explicit paths, service account, Python base, ports and environment files before reusing it. The host timer invokes its installed copy; this source commit does not update that copy automatically.

## Regression tests

```bash
python3 -B deployment/qa-autodeploy/test_autodeploy.py
```

These four unit tests isolate their filesystem state and mock subprocess/service/network boundaries; they do not operate actual services or databases. Real final-release installation, startup against database copies, deployment and endpoint read-back are separate operational acceptance checks. Preserve their private receipts outside Git.
