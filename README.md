# Obsidian Trading Terminal

Produktions-Release fuer Windows.

## Start

1. `install.bat` ausfuehren, wenn keine EXE/portable Runtime genutzt wird.
2. Danach `OBSIDIAN.vbs` oder `start_launcher.bat` starten.
3. Beim ersten Start oeffnet sich der Setup-Wizard, falls `.env` noch fehlt.

## Wichtige Dateien

- `OBSIDIAN.vbs` - startet den Launcher ohne Konsolenfenster.
- `start_launcher.bat` - alternativer sichtbarer Starter.
- `setup_wizard.pyw` - Erstkonfiguration fuer API, Telegram und Umgebung.
- `bot_config.json` - Standardparameter der Bots.
- `env_parameter.txt` - Referenz fuer optionale `.env`-Werte.

Runtime-Daten liegen lokal in `data/` und `logs/`. Diese Ordner werden beim
Start erstellt und gehoeren nicht ins Update-Repo.
