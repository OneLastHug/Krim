"""Project authorization, trusted secret delivery and Git/egress policy values.

Wire ProjectAccessService only in the trusted host. D does not expose a tool or
API endpoint, run Git, hold a database, grant cleanup authority, or enforce the
container network. See D-progress.md for integration evidence and remaining work.
"""
