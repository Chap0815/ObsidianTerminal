# Security policy

## Scope

Security issues include credential disclosure, unintended order authority,
unsafe update payloads, authentication bypass, path traversal and exposure of
private runtime data. Financial loss alone does not establish a security
vulnerability, but unexpected order behavior deserves a carefully scoped report.

The current main-branch code is the maintenance target. No long-term-support
branches or guaranteed response times are promised.

## Report privately

Do not disclose API keys, tokens, signed URLs, account identifiers or exploitable
security details in public issues or discussions.

Use [Report a vulnerability](https://github.com/Chap0815/ObsidianTerminal/security/advisories/new)
under this repository's Security tab. Private vulnerability reporting is enabled.
If that channel is unavailable, do not assume an issue
is private: request a private reporting channel from the maintainer without
including exploit details or sensitive attachments.

For exposed exchange credentials, revoke or rotate them at the exchange
immediately. Removing a Git commit or deleting an issue is not sufficient to
invalidate a credential.

Include a minimal description, affected version/commit, prerequisites and a
sanitized reproduction. Prefer synthetic data and simulation. Do not test
against another person's account, infrastructure or funds.

## Operating precautions

- Never grant withdrawal permissions to the trading API key.
- Use exchange-supported IP restrictions and account isolation where appropriate.
- Keep credentials, databases, logs, captures and backups outside Git.
- Keep the dashboard on localhost unless you understand and restrict LAN access.
- Keep HTTPS certificate verification enabled for updates.
- Review dependency and update changes before using them with real funds.

The software and these precautions do not guarantee security or prevent all
losses. See [safe operation](manual/Operating-Safely.md) and [LICENSE](LICENSE).
