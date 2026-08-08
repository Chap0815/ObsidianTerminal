"""

  OBSIDIAN TRADING TERMINAL  -  One-Click Installer (Python 3.12.10)


Einmal ausfuehren -> installiert ALLES was der Bot braucht:

  python install.py

Was es macht:
  1. Prueft Python-Version (empfohlen: 3.12.10; erlaubt 3.10 - 3.12)
  2. Erstellt/benutzt .venv im Projektordner
  3. Aktualisiert pip/setuptools/wheel im .venv
  4. Installiert alle Pflicht-Pakete hashgeprueft aus requirements.lock.txt
     im .venv (exakt gepinnte, freigegebene Versionen)
  5. VERIFIZIERT jeden Pflicht-Import in einem frischen Subprozess
     (alle Pflicht-Module importierbar)
  6. Prueft Git for Windows; installiert es bei Bedarf via winget
  7. Prueft Ollama; bietet optional Auto-Installation an
     (Windows: winget - macOS: brew, sonst manueller Hinweis)
  8. Laedt das LLM-Modell (Name aus bot_config.json -> LLM_MODEL)
  9. Legt Ordnerstruktur an (data/, logs/, prompts/)
 10. Prueft .env (sonst Hinweis auf setup_wizard.pyw)

Mehrfach ausfuehrbar  -  ueberspringt bereits Erledigtes.

"""

from __future__ import annotations

import base64
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


#  Konsole / Farben
class C:
    G = "\033[92m"
    Y = "\033[93m"
    R = "\033[91m"
    B = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


if platform.system() == "Windows":
    os.system("")  # ANSI-Farben im klassischen cmd aktivieren


def ok(m):
    print(f"  {C.G}{C.END} {m}")


def warn(m):
    print(f"  {C.Y}!{C.END} {m}")


def err(m):
    print(f"  {C.R}{C.END} {m}")


def info(m):
    print(f"  {C.B}-{C.END} {m}")


def head(m):
    print(f"\n{C.BOLD}{C.B}{m}{C.END}")


PROJECT_ROOT = Path(__file__).parent.resolve()
REQ_FILE = PROJECT_ROOT / "requirements.lock.txt"
VENV_DIR = PROJECT_ROOT / ".venv"
INSTALL_CONFIG_MAX_BYTES = 2 * 1024 * 1024
INSTALL_ENV_MAX_BYTES = 1024 * 1024

# Empfohlene Zielversion
TARGET_PY = (3, 12, 10)

#  Pflicht-Pakete (PyPI-Name, Import-Name)  -  Fallback, falls die
#    requirements-Datei fehlt. Indikatoren laufen nativ (kein pandas-ta).
REQUIRED = [
    ("numpy<2", "numpy"),
    ("pandas==2.2.2", "pandas"),
    ("ccxt>=4.3.0,<5.0.0", "ccxt"),
    ("requests>=2.31.0", "requests"),
    ("python-dotenv>=1.0.1", "dotenv"),
    ("portalocker>=2.8.2", "portalocker"),
    ("ollama>=0.3.0", "ollama"),
    ("customtkinter>=5.2.2", "customtkinter"),
    ("streamlit>=1.36.0", "streamlit"),
    ("plotly>=5.22.0", "plotly"),
    ("psutil>=5.9.8", "psutil"),
    ("tzdata>=2024.1", "tzdata"),
]

# Public CCXT-Pro WebSockets ship inside ``ccxt`` rather than as a separate
# PyPI package. Verify both the submodule and its async transport explicitly so
# an installation cannot report success while L2 collection is unusable.
REQUIRED_IMPORTS = [import_name for _spec, import_name in REQUIRED] + [
    "ccxt.pro",
    "aiohttp",
]

DEFAULT_MODEL = "qwen2.5:14b"


def _read_install_text(
    path: Path, max_bytes: int, label: str, *, errors: str = "strict"
) -> str:
    with path.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError(f"{label} exceeds size limit ({max_bytes} bytes)")
    return raw.decode("utf-8-sig", errors=errors)


#
# Schritte
#


def _venv_python() -> Path:
    if platform.system() == "Windows":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _same_path(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except Exception:
        return os.path.normcase(str(a)) == os.path.normcase(str(b))


def _running_in_project_venv() -> bool:
    try:
        exe = Path(sys.executable)
        return (
            _same_path(exe, _venv_python())
            or _same_path(Path(sys.prefix), VENV_DIR)
            or VENV_DIR.resolve() in exe.resolve().parents
        )
    except Exception:
        return False


def ensure_project_venv() -> None:
    head("[2/10] Lokales .venv vorbereiten")
    if _running_in_project_venv():
        ok(f"nutze .venv: {sys.executable}")
        return
    if os.getenv("OBSIDIAN_INSTALL_VENV") == "1":
        err(
            "Installer wurde im .venv neu gestartet, laeuft aber nicht daraus. Abbruch gegen Neustart-Schleife."
        )
        sys.exit(1)

    py = _venv_python()
    if not py.exists():
        info(f"erstelle .venv in {VENV_DIR}")
        r = subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)])
        if r.returncode != 0 or not py.exists():
            err(".venv konnte nicht erstellt werden.")
            sys.exit(1)
    else:
        ok(".venv existiert bereits")

    info("starte Installer im .venv neu ...")
    env = os.environ.copy()
    env["OBSIDIAN_INSTALL_VENV"] = "1"
    r = subprocess.run([str(py), str(Path(__file__).resolve())], env=env)
    sys.exit(r.returncode)


def check_python() -> bool:
    head("[1/10] Python-Version pruefen")
    v = sys.version_info
    cur = f"{v.major}.{v.minor}.{v.micro}"
    if v.major != 3 or v.minor < 10:
        err(f"Python {cur} ist zu alt  -  benoetigt 3.10-3.12, empfohlen 3.12.10.")
        err("Installiere Python 3.12.10 von https://www.python.org/downloads/")
        return False
    if v.minor > 12:
        err(f"Python {cur} ist neuer als der getestete Bereich 3.10-3.12.")
        err("Installiere Python 3.12.10 und starte install.bat erneut.")
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
        capture_output=True,
        text=True,
    )
    return r.returncode == 0, (r.stderr or "").strip()


def upgrade_pip() -> bool:
    head("[3/10] pip / setuptools / wheel aktualisieren")
    info("aktualisiere Build-Tooling (still) ...")
    r = _pip("install", "--upgrade", "pip", "setuptools", "wheel", "--quiet")
    if r.returncode == 0:
        ok("pip/setuptools/wheel aktuell")
        return True
    warn("pip-Upgrade nicht vollstaendig  -  fahre trotzdem fort.")
    return True


def install_packages() -> bool:
    head("[4/10] Pflicht-Pakete installieren")
    if not REQ_FILE.exists():
        err(f"{REQ_FILE.name} fehlt  -  Release ist unvollstaendig.")
        return False
    info(f"installiere aus {REQ_FILE.name} (exakt gepinnt + Hashpruefung) ...")
    r = _pip(
        "install",
        "--require-hashes", "-r", str(REQ_FILE),
        capture=False,
    )
    if r.returncode != 0:
        err("Hash-gepruefte Paketinstallation fehlgeschlagen.")
        err("Kein Fallback auf ungepruefte oder abweichende Versionen.")
        return False
    ok("Alle Pakete aus requirements.lock.txt installiert")
    return True


def verify_imports() -> bool:
    head("[5/10] Pflicht-Importe verifizieren (frischer Subprozess)")
    all_ok = True
    for import_name in REQUIRED_IMPORTS:
        good, errtxt = _can_import(import_name)
        if good:
            ok(f"import {import_name}")
        else:
            all_ok = False
            err(f"import {import_name} schlaegt fehl")
            if errtxt:
                print(f"      {errtxt.splitlines()[-1][:200]}")
    if not all_ok:
        err(
            "Mindestens ein Pflicht-Import scheitert  -  der Bot wuerde NICHT "
            "korrekt laufen. Bitte Hinweise oben befolgen und erneut starten."
        )
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
    if not _confirm("Desktop-Verknuepfung mit Obsidian-Icon anlegen"):
        return
    lnk = os.path.join(
        os.path.expanduser("~"), "Desktop", "Obsidian Trading Terminal.lnk"
    )
    vbs = os.path.join(PROJECT_ROOT, "OBSIDIAN.vbs")
    ico = os.path.join(PROJECT_ROOT, "launcher", "ui", "components", "obsidian.ico")

    def ps_quote(value: str | Path) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    vbs_arg = f'"{vbs}"'
    ps = (
        f"$s=(New-Object -ComObject WScript.Shell).CreateShortcut({ps_quote(lnk)});"
        f"$s.TargetPath='wscript.exe';$s.Arguments={ps_quote(vbs_arg)};"
        f"$s.WorkingDirectory={ps_quote(PROJECT_ROOT)};$s.IconLocation={ps_quote(f'{ico}, 0')};"
        f"$s.Description='Obsidian Trading Terminal';$s.Save()"
    )
    encoded = base64.b64encode(ps.encode("utf-16le")).decode("ascii")
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
            check=True,
            capture_output=True,
        )
        ok(f"Verknuepfung angelegt: {lnk}")
    except Exception as e:
        warn(f"Verknuepfung konnte nicht angelegt werden: {e}")


def _install_ollama() -> bool:
    """Versucht Ollama plattformabhaengig zu installieren (mit Rueckfrage)."""
    system = platform.system()
    if not _confirm("Ollama ist nicht installiert. Jetzt automatisch installieren"):
        warn("uebersprungen. Manuell: https://ollama.com/download")
        return False
    try:
        if system == "Windows":
            if shutil.which("winget"):
                info("installiere Ollama via winget ...")
                r = subprocess.run(
                    [
                        "winget",
                        "install",
                        "-e",
                        "--id",
                        "Ollama.Ollama",
                        "--accept-source-agreements",
                        "--accept-package-agreements",
                    ],
                )
                return r.returncode == 0
            warn(
                "winget nicht gefunden  -  bitte manuell installieren: "
                "https://ollama.com/download"
            )
            return False
        if system == "Darwin":
            if shutil.which("brew"):
                info("installiere Ollama via Homebrew ...")
                return subprocess.run(["brew", "install", "ollama"]).returncode == 0
            warn(
                "Homebrew nicht gefunden - bitte Ollama manuell installieren: https://ollama.com/download"
            )
            return False
        warn("Bitte Ollama manuell installieren: https://ollama.com/download")
        return False
    except Exception as e:
        err(f"Ollama-Auto-Installation fehlgeschlagen: {e}")
        return False


def check_ollama() -> bool:
    head("[7/10] Ollama pruefen")
    if not shutil.which("ollama"):
        if _install_ollama():
            ok("Ollama installiert")
        else:
            warn("Ohne Ollama laeuft der Bot weiter (Keyword-Fallback statt LLM).")
            return False
    ok(f"Ollama gefunden: {shutil.which('ollama')}")
    try:
        r = subprocess.run(
            ["ollama", "list"], capture_output=True, text=True, timeout=15
        )
        if r.returncode == 0:
            ok("Ollama-Daemon antwortet")
            return True
        warn(
            "Ollama installiert, aber Daemon antwortet nicht  -  starte die "
            "Ollama-App bzw. 'ollama serve' und fuehre install.py erneut aus."
        )
        return False
    except Exception as e:
        warn(f"Ollama-Check fehlgeschlagen: {e}")
        return False


def ensure_git() -> bool:
    head("[6/10] Git for Windows pruefen")
    try:
        from tools.ensure_git import find_git, install_git
    except Exception as exc:
        warn(f"Git-Check nicht verfuegbar: {exc}")
        return False
    git = find_git()
    if git:
        ok(f"Git gefunden: {git}")
        return True
    if platform.system() == "Windows" and shutil.which("winget"):
        info("Git nicht gefunden  -  installiere Git for Windows via winget ...")
        if install_git():
            ok(f"Git installiert: {find_git()}")
            return True
    warn("Git fehlt. Auto-Updates brauchen Git for Windows.")
    warn("Manuell: winget install -e --id Git.Git")
    return False


def _model_from_config() -> str:
    """Liest LLM_MODEL aus bot_config.json."""
    cfg_path = PROJECT_ROOT / "bot_config.json"
    try:
        if cfg_path.exists():
            data = json.loads(_read_install_text(
                cfg_path,
                INSTALL_CONFIG_MAX_BYTES,
                "bot_config.json",
            ))
            if not isinstance(data, dict):
                raise ValueError("bot_config.json root must be an object")
            raw_model = data.get("LLM_MODEL")
            if not isinstance(raw_model, str):
                raise ValueError("LLM_MODEL must be a string")
            m = raw_model.strip()
            if (
                1 <= len(m) <= 200
                and all(ch.isprintable() and not ch.isspace() for ch in m)
            ):
                return m
            raise ValueError("LLM_MODEL has an invalid format")
    except Exception as e:
        warn(f"bot_config.json nicht lesbar ({e})  -  nutze Default {DEFAULT_MODEL}")
    return DEFAULT_MODEL


def pull_model(ollama_ready: bool) -> bool:
    head("[8/10] LLM-Modell laden")
    if not ollama_ready:
        warn("uebersprungen (Ollama nicht bereit).")
        return False
    model = _model_from_config()
    info(f"Modell laut bot_config.json: {model}")
    try:
        r = subprocess.run(
            ["ollama", "list"], capture_output=True, text=True, timeout=15
        )
        installed = {
            line.split()[0]
            for line in (r.stdout or "").splitlines()
            if line.strip() and not line.lower().startswith("name")
        }
        if model in installed:
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
    head("[9/10] Ordnerstruktur anlegen")
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
    head("[10/10] Konfiguration pruefen")
    env = PROJECT_ROOT / ".env"
    if not env.exists():
        warn(".env existiert noch NICHT.")
        warn("Setup-Wizard ausfuehren:  python setup_wizard.pyw")
        return True
    ok(".env gefunden")
    try:
        content = _read_install_text(
            env,
            INSTALL_ENV_MAX_BYTES,
            ".env",
            errors="ignore",
        )
    except (OSError, ValueError) as exc:
        warn(f".env nicht lesbar ({exc})")
        return True
    lines = content.splitlines()
    api_present = any(
        ln.startswith(("API_KEY=", "BITGET_API_KEY=")) and ln.split("=", 1)[1].strip()
        for ln in lines
    )
    tz_present = any(
        ln.startswith("BOT_TIMEZONE=") and ln.split("=", 1)[1].strip() for ln in lines
    )
    (ok if api_present else warn)(
        f"Exchange API-Key {'gesetzt' if api_present else 'fehlt/leer'}"
    )
    (ok if tz_present else warn)(
        f"Zeitzone {'gesetzt' if tz_present else 'fehlt/leer'}"
    )
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
    ensure_project_venv()

    steps: list[tuple[str, bool]] = []
    upgrade_pip()
    steps.append(("Pakete", install_packages()))
    steps.append(("Imports", verify_imports()))
    steps.append(("Git", ensure_git()))
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
        print(
            f"{C.R}{C.BOLD}   Installation unvollstaendig  -  Pakete/Importe "
            f"fehlgeschlagen. Bitte Hinweise oben befolgen.{C.END}"
        )
    elif all(s for _, s in steps):
        print(
            f"{C.G}{C.BOLD}   Alles bereit! Start:  Doppelklick auf OBSIDIAN.vbs "
            f"(oder launcher.pyw){C.END}"
        )
    else:
        print(
            f"{C.Y}{C.BOLD}  ! Kern installiert; einzelne optionale Schritte "
            f"offen (z. B. Ollama/Modell). Bot ist startfaehig.{C.END}"
        )
        print(
            f"{C.Y}    Start:  Doppelklick auf OBSIDIAN.vbs (oder launcher.pyw){C.END}"
        )

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
