# Licensing and trading risk

Obsidian Trading Terminal is publicly available **source-available software**
for noncommercial use, with an explicit additional permission for personal
trading with your own funds. AI integration is optional; the same licensing
terms apply whether you enable it or run without an LLM.

The authoritative project terms are in [LICENSE](../LICENSE), which incorporates
the unchanged [PolyForm Noncommercial License 1.0.0](../LICENSES/PolyForm-Noncommercial-1.0.0.md)
and adds the personal-trading permission. This page explains those terms; it
does not create an alternative license or override either document.

## What you can do

| Activity | How it is covered |
| --- | --- |
| Read the code, learn from it, and experiment for noncommercial purposes | The base license permits noncommercial use and changes. |
| Run personal simulations and evaluate strategies | Covered by the base license and the personal-trading permission. |
| Run live trading personally with your own funds in your own account | Explicitly covered by the additional permission. Seeking or receiving personal trading gains is allowed. |
| Modify and share the project for purposes permitted by the license | Follow the base license's changes, distribution, and notice provisions; retain the root license and base license. |
| Use the project as an organization named in the base license's Noncommercial Organizations section | That section supplies its own permission; the personal-trading extension does not narrow it. |

Private trading permission applies to a natural person acting personally. It
does not extend to professional services, trading for a company, managing
client accounts, operating with third-party funds, or selling or commercially
hosting the software. The project does not offer a general commercial-use
license. Do not assume that calling an activity a hobby makes an otherwise
commercial service eligible.

The base license remains relevant independently of the personal extension.
In particular, the extension's exclusions do not cancel rights already
provided by the base license or by law. A proposed business, distribution, or
organization use should be assessed against the actual terms, not just this
table.

## Why this is called source-available

The source can be inspected and used under the published terms, but commercial
use is restricted. Consequently, this is not an OSI-approved open-source
license. Public visibility on GitHub is not equivalent to unrestricted reuse.
The [Open Source Definition](https://opensource.org/osd) requires permissions
that a noncommercial license does not grant. GitHub explains the distinction
between public access and licensing in its
[repository licensing guide](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository).

## Simulation first; live trading at your own risk

Begin in simulation and understand the configuration before enabling live
execution. Live mode can submit real exchange orders. Bugs, stale or missing
data, interrupted connectivity, exchange outages, liquidity, slippage, fees,
and liquidation can cause losses. Trading safeguards reduce particular risks;
they cannot guarantee safety or that losses will stay within an intended
limit. Leveraged exposure can increase losses rapidly.

Simulation results do not establish achievable live returns. Neither the
software nor its documentation promises profits, capital preservation,
continuous availability, or a particular execution result. AI output, when
enabled, is not a guarantee of accuracy or performance. Publishing the project
does not provide investment advice, account management, or monitoring of your
trades.

You choose your configuration and whether to enable live execution. You are
responsible for supervising your account, securing credentials, understanding
your exchange's order and margin rules, and meeting applicable legal, tax,
and exchange requirements. A software permission is not regulatory approval
to trade or to offer a financial service.

## Warranty and legal limits

The base license provides the software as-is and limits warranty and liability
to the extent applicable law allows. The root license applies that provision
to the personal-trading permission and expressly preserves rights and
liabilities that cannot lawfully be excluded.

This is not a promise that every liability can be waived, or that the terms
have been validated for every jurisdiction and use case. For example,
[German Civil Code section 309](https://www.gesetze-im-internet.de/bgb/__309.html)
places limits on certain exclusions in standard terms. This reference does
not select German law or establish which law applies to you. Obtain qualified
advice for questions that depend on your circumstances.

## Dependencies, models, and contributions

Third-party libraries, Python and other runtime components, model weights,
and separately licensed assets retain their own licenses. Exchange APIs and
external services have their own terms. The project's noncommercial license
does not relicense those materials or grant rights their owners have not
provided. The dependency list is not itself a complete third-party license
inventory; check the notices and licenses of the versions you redistribute.

The official repository is owner-maintained and does not accept external code
contributions; see the [maintenance policy](../CONTRIBUTING.md). Local
modifications remain subject to the license. This repository policy does not
withdraw any permissions independently granted by that license. Other authors
retain their copyrights; the root license does not assert ownership of their
work or erase separately applicable notices.

The PolyForm text is reproduced from its
[official published 1.0.0 text](https://polyformproject.org/licenses/noncommercial/1.0.0.txt).
The personal-trading extension is a separate project permission; it is not
part of the standard PolyForm text or an endorsement by the PolyForm Project.
