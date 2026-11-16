"""Project authorization, sealed delivery and native Git host adapters.

Wire ProjectAccessService and TrustedGitRunner only in the trusted host. This
package does not expose model tools, own a database, grant cleanup authority,
or enforce container networking. See deployment/development-credentials/README.md
for assembly interfaces and local verification scope.
"""
