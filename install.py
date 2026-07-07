"""

  OBSIDIAN TRADING TERMINAL  -  One-Click Installer (Python 3.12.10)


Einmal ausfhren -> installiert ALLES was der Bot braucht:

  python install.py

Was es macht:
  1. Prft Python-Version (empfohlen: 3.12.10; erlaubt 3.10 - 3.12)
  2. Aktualisiert pip/setuptools/wheel
  3. Installiert alle Pflicht-Pakete aus requirements.txt
     (gepinnte, kompatible Versionen; Indikatoren laufen nativ)
  4. VERIFIZIERT jeden Pflicht-Import in einem frischen Subprozess
     (alle Pflicht-Module importierbar)
  5. Prft Ollama; bietet optional Auto-Installation an
     (Windows: winget - macOS: brew/Script - Linux: curl-Script)
  6. Ldt das LLM-Modell (Name aus bot_config.json -> LLM_MODEL)
  7. Legt Ordnerstruktur an (data/, logs/, prompts/)
  8. Prft .env (sonst Hinweis auf setup_wizard.pyw)

Mehrfach ausfhrbar  -  berspringt bereits Erledigtes.

"""
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


#  Konsole / Farben 
class C:
    G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; B = "\033[96m"
    BOLD = "\033[1m"; END = "\033[0m"


if platform.system() == "Windows":
    os.system("")          # ANSI-Farben im klassischen cmd aktivieren


def ok(m):   print(f"  {C.G}{C.END} {m}")
def warn(m): print(f"  {C.Y}!{C.END} {m}")
def err(m):  print(f"  {C.R}{C.END} {m}")
def info(m): print(f"  {C.B}-{C.END} {m}")
def head(m): print(f"\n{C.BOLD}{C.B}{m}{C.END}")


PROJECT_ROOT = Path(__file__).parent.resolve()
REQ_FILE = PROJECT_ROOT / "requirements.txt"

# Empfohlene Zielversion
TARGET_PY = (3, 12, 10)

#  Pflicht-Pakete (PyPI-Name, Import-Name)  -  Fallback, falls die
#    requirements-Datei fehlt. Indikatoren laufen nativ (kein pandas-ta). 
REQUIRED = [
    ("numpy<2",            "numpy"),
    ("pandas==2.2.2",      "pandas"),
    ("ccxt>=4.3.0,<5.0.0", "ccxt"),
    ("requests>=2.31.0",   "requests"),
    ("python-dotenv>=1.0.1", "dotenv"),
    ("portalocker>=2.8.2", "portalocker"),
    ("ollama>=0.3.0",      "ollama"),
    ("customtkinter>=5.2.2", "customtkinter"),
    ("streamlit>=1.36.0",  "streamlit"),
    ("plotly>=5.22.0",     "plotly"),
    ("psutil>=5.9.8",      "psutil"),
    ("tzdata>=2024.1",     "tzdata"),
]

DEFAULT_MODEL = "qwen2.5:14b"


# 
# Schritte
# 

def check_python() -> bool:
    head("[1/8] Python-Version prfen")
    v = sys.version_info
    cur = f"{v.major}.{v.minor}.{v.micro}"
    if v.major != 3 or v.minor < 10:
        err(f"Python {cur} ist zu alt  -  bentigt 3.10+, empfohlen 3.12.10.")
        err("Installiere Python 3.12.10 von https://www.python.org/downloads/")
        return False
    if (v.major, v.minor) == (3, 12):
        ok(f"Python {cur}  -  OK (Zielversion)")
    else:
        warn(f"Python {cur}  -  funktioniert, empfohlen ist 3.12.10.")
        ok("Fortfahren ...")
    return True


def _pip(*args, capture=True) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "pip", *args]
    return subprocess.run(cmd, capture_output=capture, text=True)


def _can_import(module_name: str) -> tuple[bool, str]:
    """Importiert das Modul in einem FRISCHEN Subprozess (echte Verifikation)."""
    r = subprocess.run(
        [sys.executable, "-c", f"import {module_name}"],
        capture_output=True, text=True,
    )
    return r.returncode == 0, (r.stderr or "").strip()


def upgrade_pip() -> bool:
    head("[2/8] pip / setuptools / wheel aktualisieren")
    info("aktualisiere Build-Tooling (still) ...")
    r = _pip("install", "--upgrade", "pip", "setuptools", "wheel", "--quiet")
    if r.returncode == 0:
        ok("pip/setuptools/wheel aktuell")
        return True
    warn("pip-Upgrade nicht vollstndig  -  fahre trotzdem fort.")
    return True


def install_packages() -> bool:
    head("[3/8] Pflicht-Pakete installieren")
    if REQ_FILE.exists():
        info(f"installiere aus {REQ_FILE.name} (gepinnte, kompatible Versionen) ...")
        r = _pip("install", "-r", str(REQ_FILE), capture=False)
        if r.returncode == 0:
            ok("Alle Pakete aus requirements.txt installiert")
            return True
        warn("requirements-Datei-Installation unvollstaendig  -  versuche Einzel-Pakete ...")

    # Fallback: Einzelinstallation
    failed = []
    for spec, import_name in REQUIRED:
        info(f"{spec} ...")
        r = _pip("install", spec, "--quiet")
        if r.returncode == 0:
            ok(f"{spec}")
        else:
            err(f"{spec}  -  FEHLER")
            tail = (r.stderr or "")[-200:].strip()
            if tail:
                print(f"      {tail}")
            failed.append(spec)
    if failed:
        warn(f"{len(failed)} Paket(e) fehlgeschlagen: {', '.join(failed)}")
        return False
    return True


def verify_imports() -> bool:
    head("[4/8] Pflicht-Importe verifizieren (frischer Subprozess)")
    all_ok = True
    for _, import_name in REQUIRED:
        good, errtxt = _can_import(import_name)
        if good:
            ok(f"import {import_name}")
        else:
            all_ok = False
            err(f"import {import_name} schlgt fehl")
            if errtxt:
                print(f"      {errtxt.splitlines()[-1][:200]}")
    if not all_ok:
        err("Mindestens ein Pflicht-Import scheitert  -  der Bot wrde NICHT "
            "korrekt laufen. Bitte Hinweise oben befolgen und erneut starten.")
    return all_ok


#  Ollama 

def _confirm(question: str) -> bool:
    try:
        ans = input(f"  {C.Y}{C.END} {question} [j/N] ").strip().lower()
    except EOFError:
        return False
    return ans in ("j", "ja", "y", "yes")


def offer_desktop_shortcut() -> None:
    """OPT-IN (default no): create a desktop shortcut to OBSIDIAN.vbs with the
    Obsidian icon. A .vbs can't carry an icon itself; a .lnk can. Only runs on
    Windows and only when the user says yes  -  nothing is created uninvited."""
    if platform.system() != "Windows":
        return
    if not _confirm("Desktop-Verknpfung mit Obsidian-Icon anlegen"):
        return
    lnk = os.path.join(os.path.expanduser("~"), "Desktop",
                       "Obsidian Trading Terminal.lnk")
    vbs = os.path.join(PROJECT_ROOT, "OBSIDIAN.vbs")
    ico = os.path.join(PROJECT_ROOT, "launcher", "ui", "components", "obsidian.ico")
    ps = (
        f"$s=(New-Object -ComObject WScript.Shell).CreateShortcut('{lnk}');"
        f"$s.TargetPath='wscript.exe';$s.Arguments='\"{vbs}\"';"
        f"$s.WorkingDirectory='{PROJECT_ROOT}';$s.IconLocation='{ico}, 0';"
        f"$s.Description='Obsidian Trading Terminal';$s.Save()"
    )
    try:
        subprocess.run(["powershell", "-NoProfile", "-NonInteractive",
                        "-Command", ps], check=True, capture_output=True)
        ok(f"Verknpfung angelegt: {lnk}")
    except Exception as e:
        warn(f"Verknpfung konnte nicht angelegt werden: {e}")


def _install_ollama() -> bool:
    """Versucht Ollama plattformabhngig zu installieren (mit Rckfrage)."""
    system = platform.system()
    if not _confirm("Ollama ist nicht installiert. Jetzt automatisch installieren"):
        warn("uebersprungen. Manuell: https://ollama.com/download")
        return False
    try:
        if system == "Windows":
            if shutil.which("winget"):
                info("installiere Ollama via winget ...")
                r = subprocess.run(
                    ["winget", "install", "-e", "--id", "Ollama.Ollama",
                     "--accept-source-agreements", "--accept-package-agreements"],
                )
                return r.returncode == 0
            warn("winget nicht gefunden  -  bitte manuell installieren: "
                 "https://ollama.com/download")
            return False
        if system == "Darwin":
            if shutil.which("brew"):
                info("installiere Ollama via Homebrew ...")
                return subprocess.run(["brew", "install", "ollama"]).returncode == 0
            info("installiere Ollama via offiziellem Script ...")
            return subprocess.run(
                "curl -fsSL https://ollama.com/install.sh | sh", shell=True
            ).returncode == 0
        # Linux
        info("installiere Ollama via offiziellem Script ...")
        return subprocess.run(
            "curl -fsSL https://ollama.com/install.sh | sh", shell=True
        ).returncode == 0
    except Exception as e:
        err(f"Ollama-Auto-Installation fehlgeschlagen: {e}")
        return False


def check_ollama() -> bool:
    head("[5/8] Ollama prfen")
    if not shutil.which("ollama"):
        if _install_ollama():
            ok("Ollama installiert")
        else:
            warn("Ohne Ollama laeuft der Bot weiter (Keyword-Fallback statt LLM).")
            return False
    ok(f"Ollama gefunden: {shutil.which('ollama')}")
    try:
        r = subprocess.run(["ollama", "list"], capture_output=True,
                           text=True, timeout=15)
        if r.returncode == 0:
            ok("Ollama-Daemon antwortet")
            return True
        warn("Ollama installiert, aber Daemon antwortet nicht  -  starte die "
             "Ollama-App bzw. 'ollama serve' und fhre install.py erneut aus.")
        return False
    except Exception as e:
        warn(f"Ollama-Check fehlgeschlagen: {e}")
        return False


def _model_from_config() -> str:
    """Liest LLM_MODEL aus bot_config.json."""
    cfg_path = PROJECT_ROOT / "bot_config.json"
    try:
        if cfg_path.exists():
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            m = str(data.get("LLM_MODEL", "")).strip()
            if m:
                return m
    except Exception as e:
        warn(f"bot_config.json nicht lesbar ({e})  -  nutze Default {DEFAULT_MODEL}")
    return DEFAULT_MODEL


def pull_model(ollama_ready: bool) -> bool:
    head("[6/8] LLM-Modell laden")
    if not ollama_ready:
        warn("uebersprungen (Ollama nicht bereit).")
        return False
    model = _model_from_config()
    info(f"Modell laut bot_config.json: {model}")
    try:
        r = subprocess.run(["ollama", "list"], capture_output=True,
                           text=True, timeout=15)
        if model.split(":")[0] in (r.stdout or ""):
            ok(f"{model}  -  bereits vorhanden")
            return True
    except Exception:
        pass
    info(f"{model} wird geladen (mehrere GB, kann dauern) ...\n")
    try:
        # Live-Fortschritt durchreichen
        r = subprocess.run(["ollama", "pull", model])
        if r.returncode == 0:
            ok(f"{model}  -  geladen")
            return True
        err(f"{model}  -  Download fehlgeschlagen")
        return False
    except Exception as e:
        err(f"Modell-Download fehlgeschlagen: {e}")
        return False


def create_folders() -> bool:
    head("[7/8] Ordnerstruktur anlegen")
    folders = ["data", "logs", "logs/Spot", "logs/Trend", "logs/Futures", "prompts"]
    for f in folders:
        p = PROJECT_ROOT / f
        if p.exists():
            ok(f"{f}/  -  existiert")
        else:
            p.mkdir(parents=True, exist_ok=True)
            ok(f"{f}/  -  erstellt")
    return True


def check_env() -> bool:
    head("[8/8] Konfiguration prfen")
    env = PROJECT_ROOT / ".env"
    if not env.exists():
        warn(".env existiert noch NICHT.")
        warn("Setup-Wizard ausfhren:  python setup_wizard.pyw")
        return True
    ok(".env gefunden")
    content = env.read_text(encoding="utf-8", errors="ignore")
    for key, label in (("BITGET_API_KEY", "Bitget API-Key"),
                       ("BOT_TIMEZONE", "Zeitzone")):
        present = any(
            ln.startswith(f"{key}=") and ln.split("=", 1)[1].strip()
            for ln in content.splitlines()
        )
        (ok if present else warn)(
            f"{label} {'gesetzt' if present else 'fehlt/leer'}")
    return True


# 

def main() -> None:
    print(f"""
{C.BOLD}{C.B}
   OBSIDIAN TRADING TERMINAL  -  Installer (Python 3.12)    
{C.END}
""")
    info(f"Projekt: {PROJECT_ROOT}")

    if not check_python():
        err("\nAbbruch  -  inkompatible Python-Version.")
        input("\nEnter zum Beenden ...")
        return

    steps: list[tuple[str, bool]] = []
    upgrade_pip()
    steps.append(("Pakete", install_packages()))
    steps.append(("Imports", verify_imports()))
    ollama_ready = check_ollama()
    steps.append(("Ollama", ollama_ready))
    steps.append(("Modell", pull_model(ollama_ready)))
    steps.append(("Ordner", create_folders()))
    steps.append(("Config", check_env()))

    head("ZUSAMMENFASSUNG")
    hard_fail = False
    for name, success in steps:
        (ok if success else err)(name)
        # Pakete + Imports sind harte Voraussetzungen
        if not success and name in ("Pakete", "Imports"):
            hard_fail = True

    print()
    if hard_fail:
        print(f"{C.R}{C.BOLD}   Installation unvollstaendig  -  Pakete/Importe "
              f"fehlgeschlagen. Bitte Hinweise oben befolgen.{C.END}")
    elif all(s for _, s in steps):
        print(f"{C.G}{C.BOLD}   Alles bereit! Start:  Doppelklick auf OBSIDIAN.vbs "
              f"(oder launcher.pyw){C.END}")
    else:
        print(f"{C.Y}{C.BOLD}  ! Kern installiert; einzelne optionale Schritte "
              f"offen (z. B. Ollama/Modell). Bot ist startfhig.{C.END}")
        print(f"{C.Y}    Start:  Doppelklick auf OBSIDIAN.vbs (oder launcher.pyw){C.END}")

    if not hard_fail:
        print()
        offer_desktop_shortcut()

    print()
    input("Enter zum Beenden ...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAbgebrochen.")
    except Exception as e:
        print(f"\n{C.R}Unerwarteter Fehler: {e}{C.END}")
        input("\nEnter zum Beenden ...")
