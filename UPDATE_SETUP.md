# Update-Zugriff einrichten

Der Updater nutzt pro Rechner einen eigenen read-only SSH Deploy Key.
Der Private Key wird niemals mit dem Installer ausgeliefert.

## 1. Key auf dem User-Rechner erstellen

In CMD:

```cmd
mkdir "%USERPROFILE%\.ssh"
ssh-keygen -t ed25519 -f "%USERPROFILE%\.ssh\obsidian_update_ed25519" -C "obsidian-update"
type "%USERPROFILE%\.ssh\obsidian_update_ed25519.pub"
```

Fuer automatische Updates die Passphrase leer lassen. Der Key muss auf GitHub
trotzdem nur read-only sein.

## 2. Public Key in GitHub eintragen

Repo: `Chap0815/ObsidianTerminal`

`Settings -> Deploy keys -> Add deploy key`

Wichtig: `Allow write access` nicht aktivieren.

## 3. SSH Alias einrichten

Datei `%USERPROFILE%\.ssh\config`:

```text
Host github-obsidian
  HostName github.com
  User git
  IdentityFile ~/.ssh/obsidian_update_ed25519
  IdentitiesOnly yes
```

## 4. Verbindung testen

```cmd
ssh -T git@github-obsidian
git ls-remote git@github-obsidian:Chap0815/ObsidianTerminal.git
```

## 5. Update starten

Im Installationsordner:

```cmd
update.bat
```

Der Updater bricht ab, wenn Bots laufen, und erhaelt `.env`, `bot_config.json`,
`data/`, `logs/` und aktive Prompt-Dateien.
