# Upgrade and rollback

This runbook treats code, package, database migration and live runtime as
separate gates. A passing test suite does not prove a successful deployment.

## Preflight

1. Record the current release identifier and service unit.
2. Run `hubctl doctor`; resolve database integrity or permission failures.
3. Run `hubctl backup` and retain the reported database plus `.sha256` file.
4. Build the candidate with `python -m build` and install its wheel into a clean
   virtual environment. Do not deploy from an editable checkout.
5. Start the candidate on another port against a copy of the database. Run
   `hubctl doctor`, an MCP initialize/session/status smoke test and migration
   count checks against that copy.

## Cutover

1. Stop the user service and confirm the process is gone.
2. Switch the service to the immutable candidate release directory.
3. Run `systemctl --user daemon-reload` and start the service.
4. Verify `systemctl --user is-active agent-hub.service`, `hubctl doctor`,
   `hubctl status --json`, MCP authentication and a non-mutating status call.
5. Preserve the previous release and backup until the observation window ends.

The v0.6 migration is additive. It must still run only after a verified backup.
Never remove `hub.db`, `hub.db-wal` or `hub.db-shm` as an upgrade procedure.

## Rollback

If startup or smoke checks fail, stop the service and restore the previous code
release. The v0.5 application is expected to ignore v0.6's additive columns and
table, but the supported deterministic rollback is the previous code release
paired with the pre-cutover database backup. Restore both as one snapshot when
database behavior is in doubt.

Use SQLite backup/restore tooling; do not copy a live WAL database with ordinary
file copy. Re-run integrity, migration and permission checks before restarting.

## Release evidence

Keep these records with the release:

- source commit and Git remote;
- wheel and sdist SHA256 hashes;
- test, build and clean-install results;
- source and target migration versions;
- backup path and checksum;
- service unit diff and runtime smoke-test output;
- explicit rollback result if rollback was exercised.
