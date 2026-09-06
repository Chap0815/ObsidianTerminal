# Project maintenance and feedback

Obsidian Trading Terminal is maintained by **Chap0815**. This is a
**source-available, noncommercial** project, not an OSI open-source project.
Read [LICENSE](LICENSE) before using, modifying or distributing it.

## Start with the right channel

- Reproducible defects: [Issues](https://github.com/Chap0815/ObsidianTerminal/issues).
- Questions and design proposals: [Discussions](https://github.com/Chap0815/ObsidianTerminal/discussions).
- Vulnerabilities and exposed secrets: follow [SECURITY.md](SECURITY.md).

Bug reports, questions and suggestions are welcome. Do not include credentials,
private account details or unredacted logs in public reports.

## Official development and local modifications

The official branch and releases are maintained exclusively by the project
owner. **External code contributions and unsolicited code pull requests are not
accepted.** Opening an issue, discussion or pull request does not grant write,
merge or release access to the official repository.

You may modify your own local copy as permitted by [LICENSE](LICENSE). Those
changes do not change the official version and are not maintained or endorsed
by the project owner. This repository policy does not narrow rights to use,
modify or distribute material that the license independently grants.

The official `main` branch requires an owner-controlled pull request and the
successful publication integrity check. Direct pushes, force-pushes and branch
deletion are blocked by repository rules; no bypass actors are configured.
Version tags matching `v*` cannot be rewritten or deleted under the active rules.
These controls do not remove the owner's administrative ability to change
repository settings and are not a guarantee against account compromise.

## Maintainer development and review

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

## Before publishing

Review every staged file. Exclude `.env` and its variants, API keys, local
configuration, account screenshots, databases, logs, captures and backups.
A `.gitignore` is a convenience, not a substitute for reviewing the payload.

Explain compatibility and rollback considerations. Avoid unrelated cleanup,
generated artifacts and changes to trading defaults without supporting evidence.
