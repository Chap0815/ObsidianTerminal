# Updates

Public source updates use **HTTPS with certificate verification**. You do not
need a GitHub account, token, SSH key or paid subscription to download this public
repository. Git and a working supported Python installation are still required.

HTTPS is intentional: do not disable TLS verification, set
`GIT_SSL_NO_VERIFY`, or change the repository to plain HTTP. If a proxy or
certificate error blocks an update, fix the trust configuration or use a trusted
network. A hash manifest checks payload consistency; it is not an independent
publisher signature and cannot make a compromised repository trustworthy.

## Configure a current installation

In your installation folder, copy
[`config/update_config.example.json`](config/update_config.example.json) to
`config/update_config.json` **only if the latter does not already exist**.
If it exists, edit the repository fields without discarding local settings:

```json
{
  "repo_url": "https://github.com/Chap0815/ObsidianTerminal.git",
  "branch": "main"
}
```

The real `config/update_config.json` is local and must not be committed.
Environment overrides such as `OBSIDIAN_UPDATE_REPO_URL` can take precedence;
remove obsolete overrides deliberately if they still select an old SSH URL.
Only the official repository URLs accepted by the updater are supported.

## Before every update

1. Review [release notes](RELEASE.md). An update is not a strategy recommendation.
2. Check open positions and pending orders directly at the exchange.
3. Use the launcher's explicit close-and-stop workflow if you intend to close
   exposure. Wait for confirmed results. A stopped process alone is not proof
   that an exchange position is closed.
4. Stop all bots and close the launcher cleanly. Resolve incomplete process scans
   or shutdown errors first; do not bypass the update barrier.
5. Back up your private configuration and persistent runtime state to private
   storage while the application is stopped. Keep that backup out of Git.

If positions must remain open, you need a deliberate maintenance plan and direct
exchange supervision. A code update cannot provide monitoring while bots are off.

## Apply and verify

Run `update.bat` in the installation folder. Read its result before restarting.
The updater checks for running processes, validates the source manifest and
applies the allowed payload. Private configuration and runtime data are excluded
from the public payload; supported local files are preserved.

After success, start `start_launcher.bat` and verify:

- the displayed version/build and startup logs;
- every strategy's SIM/LIVE selection and settings;
- exchange connectivity and position reconciliation;
- the absence of new database, process-scan or persistence errors.

Do not start a second copy against the same runtime state. If an update fails,
keep the failure output private until you have removed secrets and account data.
Do not manually clear barriers, delete databases or mix files from different
builds to force a restart.

## Important: older private / SSH-only installations

An old updater does not become HTTPS-capable just because its configuration URL
changes. Older builds may also reject the root `.gitignore` now included in the
public manifest. These are compatibility checks, not a reason to disable
validation.

For that legacy transition, use a **fresh, separate source installation**:

1. Stop the old installation and confirm exchange exposure as described above.
2. Preserve a private backup of its configuration and state.
3. Download and extract the complete current source into a different dedicated
   folder, then run `install.bat`.
4. Re-enter configuration using the setup wizard. If you need to retain prior
   runtime history or open-position state, arrange a reviewed complete migration;
   do not copy a live SQLite database, selected tables or only part of its state.
5. Keep the new installation in SIM until configuration, connectivity and the
   intended state are verified. Never run both installations simultaneously.

Do not overwrite a bundled Python installation with a source ZIP or copy just
the updater script: its validation helpers evolve together. Existing SSH setups
remain an advanced option where explicitly supported, but new public users do
not need deploy keys.

[Back to the terminal](README.md) · [Troubleshooting](manual/Troubleshooting.md)
