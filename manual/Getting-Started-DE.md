# Einstieg auf Deutsch

[Projektübersicht](../README.md) · [English guide](Getting-Started.md) · [Sicherer Betrieb](Operating-Safely.md)

Obsidian Trading Terminal ist eine Windows-Desktopanwendung mit fünf Kryptowährungsstrategien, Simulation, Positions- und Ergebnisansichten sowie Analysewerkzeugen. Die Anwendung ist **AI-compatible**: KI über Ollama ist optional. Die Standardkonfiguration benötigt kein Sprachmodell.

Alle fünf öffentlichen Bot-Defaults sind SIM. LIVE ist eine bewusste Entscheidung des Betreibers, verwendet echtes Geld und erfolgt auf eigenes Risiko. Lies vor der Nutzung die [Lizenz](../LICENSE) und die [Betriebshinweise](Operating-Safely.md). Es gibt keine Gewinnzusage.

## Installation

1. Lade den Quellcode ausschließlich aus dem [offiziellen Repository](https://github.com/Chap0815/ObsidianTerminal) über **Code → Download ZIP** herunter und entpacke ihn vollständig. Alternativ:

   ```powershell
   git clone https://github.com/Chap0815/ObsidianTerminal.git
   cd ObsidianTerminal
   ```

2. Starte `install.bat`. Der Installer bevorzugt Python 3.12, akzeptiert 3.10–3.12 und empfiehlt 3.12.10. Fehlendes Python kann über Windows Package Manager installiert werden. Beachte einen möglichen Hinweis, das Installationsfenster danach neu zu öffnen.
3. Prüfe das Ergebnis. Fehler bei erforderlichen Paketen oder Importen müssen behoben werden. Der Installer richtet eine Projektumgebung mit den festgelegten Abhängigkeiten ein.
4. Ollama ist optional; du kannst seine Installation ablehnen. Ist Ollama bereits erreichbar, kann der Installer das konfigurierte Modell herunterladen. Dafür können mehrere Gigabyte erforderlich sein. Für die Standardstrategien mit deaktivierter KI ist das nicht nötig.
5. Starte `OBSIDIAN.vbs`. `start_launcher.bat` ist eine Alternative, wenn du Startmeldungen zur Diagnose sehen möchtest.

Das GitHub-Quellcode-ZIP ist kein eigenständiges Programm mit eingebettetem Python. Ein separat angebotenes Installationspaket hat gegebenenfalls einen anderen Ablauf; maßgeblich sind dessen Releasehinweise.

## Ersteinrichtung

Beim ersten Start öffnet sich der Setup-Assistent, wenn `.env` noch fehlt. Er speichert Börsenverbindung, API-Zugang und optionale Dienste lokal.

Zur Auswahl stehen Bitget, Binance, OKX, Bybit, KuCoin, Gate.io und MEXC. Die Derivateanbindung verwendet USDT-lineare Perpetuals. Die Auswahl im Assistenten garantiert weder die Verfügbarkeit in deinem Land noch die API-Handelsberechtigung deines Kontos oder die Validierung jeder Strategie auf dieser Börse.

Verwende eigene API-Zugangsdaten und aktiviere keine Auszahlungsberechtigung. Bitget, OKX und KuCoin benötigen in der Anbindung zusätzlich eine Passphrase. Proxy und Telegram sind optional. Prüfe die tatsächliche Verbindung und gegebenenfalls die Zustellung von Benachrichtigungen.

SIM bedeutet simulierte Orderausführung, nicht vollständig offline oder in jedem Fall ohne Zugangsdaten. Marktdaten und börsenspezifische Verbindungsanforderungen bleiben relevant.

## Die fünf Bots

| Bot | Markt und Verhalten | KI |
| --- | --- | --- |
| TREND | Spot-Trendfolge; long oder ohne Position | Nicht benötigt |
| SPOT | Spot-Momentum; long oder ohne Position | Optional, standardmäßig aus |
| FUTURES | Direktionales Momentum auf Perpetuals; long/short | Optional, standardmäßig aus |
| CROSS | Momentum-Rangliste mit Long-/Short-Korb und Rebalancing | Nicht benötigt |
| FUTREND | Trendfolge auf Perpetuals; long oder ohne Position | Nicht benötigt |

CROSS strebt ausgewogene Long-/Short-Exponierung an; Marktneutralität oder Verlustfreiheit sind nicht garantiert. FUTREND handelt keine Shorts. Die aktuellen öffentlichen Derivate-Defaults verwenden 1× Hebel. Einzelheiten stehen in [Strategies](Strategies.md).

## Erste Simulation

Prüfe vor dem Start alle Modusanzeigen: Sie müssen für den ersten Test **SIM** zeigen. Eine vorhandene private Konfiguration kann von den öffentlichen Defaults abweichen.

Starte zunächst einen Bot, lass Initialisierung und Datenaufbau abschließen und lies Status sowie Logs. Kontrolliere Bot, Modus und Zeitraum bei Positionen und PnL. SIM-Ergebnisse sind keine real erzielten Börsengewinne.

Ein gesunder Bot muss nicht sofort handeln. Signale, Historie, Spread, Entryfilter, Positionsgrenzen, Kontostand, Cooldowns und Zeitpläne können einen Einstieg verhindern. Bei FUTURES existiert zusätzlich `NEW_ENTRIES_ENABLED`; deaktivierte Neueinstiege lassen die Verwaltung vorhandener Positionen weiterlaufen.

Über **Open Dashboard** öffnest du die Ergebnisansicht im Browser. Neue Installationen binden sie standardmäßig nur an den lokalen Rechner. **Run Self-Test** prüft im öffentlichen Paket Integrität und Quellcode-Kompilierbarkeit; das ist weder ein Gewinntest noch eine vollständige Börsenprüfung.

## Stoppen, Schließen und Neustarten

| Aktion | Bedeutung |
| --- | --- |
| Close All & Stop | Positionen schließen und den ausgewählten Bot stoppen; Abschluss prüfen |
| Stop without closing | Bot stoppen, Positionen erhalten; die Botüberwachung endet |
| Restart | Prozess neu starten und gespeicherte Positionen wieder aufnehmen; kein Positionsschluss |
| Close All & Quit | Schließworkflow für die Anwendung; tatsächlichen Abschluss kontrollieren |

Bei LIVE können Schließaktionen echte Orders auslösen. Eine angeforderte Schließung ist noch kein bestätigter Fill. Prüfe bei einem Fehler direkt auf der Börse, welche Positionen und Orders tatsächlich bestehen.

Stop-Loss und Trailing sind lokale Entscheidungen des laufenden Bots. Ausfall, Standby, fehlende Kurse oder API-Probleme können ihre Ausführung verhindern; der eingestellte Wert garantiert keinen Ausstiegskurs. Das Beenden eines Fensters oder Python-Prozesses stellt ein Börsenkonto nicht automatisch glatt.

## Einstellungen und private Dateien

Die aktive Installation verwendet ihre eigene `.env`, `bot_config.json`, Datenbank, Logs und Zustandsdateien. Bearbeite nicht versehentlich eine zweite Quellcodekopie. Nutze die vorgesehenen Konfigurationsdialoge und beachte, dass einige Änderungen einen Neustart benötigen.

Lade keine `.env`, privaten Konfigurationen, Datenbanken, vollständigen Logs oder ungeprüften Screenshots auf GitHub hoch. Zugangsdaten, Kontodaten und persönliche Pfade können auch in Diagnosen enthalten sein. Lösche bei Fehlern keine Zustandsdateien oder Datenbankeinträge, um eine grüne Anzeige zu erzwingen.

Research-Capture und L2-Aufzeichnung sind in der User-Version standardmäßig deaktiviert. Für normalen Betrieb müssen sie nicht eingeschaltet werden. Optionale KI wird nur bei SPOT/FUTURES über `USE_LLM` aktiviert; ein gespeicherter Prompt allein schaltet sie nicht ein.

Weiterführend: [Configuration](Configuration.md), [Operating safely](Operating-Safely.md), [Troubleshooting](Troubleshooting.md), [Research and AI](Research-and-AI.md) und [Architecture](Architecture.md).
