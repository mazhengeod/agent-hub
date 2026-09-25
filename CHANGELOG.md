# Changelog

## 0.6.0

- Separate delivery observation from acknowledgement and add atomic batch ack.
- Add terminal-delivery retention, approval expiry reminders and richer health
  diagnostics without deleting task history.
- Validate structured work, dependency, artifact and event inputs at the API
  boundary and return concise MCP tool errors for domain failures.
- Add read-only `hubctl doctor` and online `hubctl backup` operations.
- Harden token comparison, token-file permissions and the systemd sandbox.
- Ship database migrations inside the `agent-hub-mcp` wheel and verify clean
  wheel installation in CI.
- Add a documented backup, staged migration, cutover and rollback procedure.
