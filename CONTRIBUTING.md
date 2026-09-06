# Contributing

Thank you for helping improve Obsidian Trading Terminal. This is a
**source-available, noncommercial** project, not an OSI open-source project.
Read [LICENSE](LICENSE) before using, modifying or distributing it.

## Start with the right channel

- Reproducible defects: [Issues](https://github.com/Chap0815/ObsidianTerminal/issues).
- Questions and design proposals: [Discussions](https://github.com/Chap0815/ObsidianTerminal/discussions).
- Vulnerabilities and exposed secrets: follow [SECURITY.md](SECURITY.md).

Discuss large changes first. Small, focused changes are easier to review,
particularly around orders, persistence, shutdown, configuration and updates.

## Contribution terms

By intentionally submitting a contribution for inclusion, you agree to license
your contribution under the project license, including its additional personal
own-account trading permission. You retain your copyright.

Submit only material you have the right to contribute. Identify third-party
code and its license; do not assume the project license replaces dependency
licenses. This policy does not retroactively claim rights to somebody else's
existing work.

## Development and review

1. Work in a separate development copy with synthetic credentials and isolated
   test data. Never develop against a running LIVE installation.
2. Reproduce the defect and describe expected versus observed behavior.
3. Keep the patch narrowly scoped. Preserve existing configuration and state.
4. Add regression tests or a reproducible verification procedure.
5. State what was tested, what was not tested and any migration consequences.

The public source payload includes a package self-test and research tools. The
maintainer's full internal regression suite is not included in this distribution.
Do not describe the package smoke check as the complete test suite.

GitHub's **Public payload integrity** workflow checks the release manifest,
source and private-file boundary using the standard-library release gate.
It does not install trading dependencies, contact an exchange or run bots.
Changes to published files require a reviewed regenerated manifest before
that gate can pass; do not remove the check to accept an inconsistent payload.

For order or accounting changes, cover retries, ambiguous exchange responses,
partial fills, persistence failures and restart behavior. For updater changes,
cover malformed paths, private-file preservation, payload tampering and failure
before copying. Never weaken a safety check merely to make a test pass.

Backtest results must identify data, fees, slippage assumptions, time ranges,
selection procedure and out-of-sample limitations. Do not present optimization
results as proof of future profit.

## Before submitting

Review every staged file. Exclude `.env` and its variants, API keys, local
configuration, account screenshots, databases, logs, captures and backups.
A `.gitignore` is a convenience, not a substitute for reviewing the payload.

Explain compatibility and rollback considerations. Avoid unrelated cleanup,
generated artifacts and changes to trading defaults without supporting evidence.
