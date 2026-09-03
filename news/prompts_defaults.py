"""
prompts_defaults.py  Embedded Default-Prompts as Fallback

    Self-healing: on first launcher start the launcher writes these into
    prompts/*.txt and prompts/*_default.txt so the user always has a working
    editable prompt plus a baseline fallback.

Kept in sync with the files in prompts/.
"""

SPOT_DEFAULT = """You are a MOMENTUM SPECIALIST. You evaluate SPOT entries for {symbol} to capture strong directional moves.
Your bias is to ENGAGE when 3+ confirmations align. Otherwise, WAIT.

=== MARKET DATA ===
Price: {change}% 24h | Regime: {regime} | BTC 24h: {btc_24h:+.2f}% | F&G: {fg} ({fg_label})
RSI: 15m={rsi_15m:.1f}, 1h={rsi_1h:.1f}, 4h={rsi_4h:.1f}
News: {news}

=== NEGATIVE CATALYST KEYWORDS ===
hack, exploit, breach, scam, fraud, rug, delist, ban, lawsuit, SEC/CFTC, arrest, bankruptcy, depeg, insider sell.
AUTO-FAIL if any of these apply directly to {symbol}.

=== CONFIRMATIONS NEEDED (need 3 of 4) ===
1. IMPULSE: 15m RSI > 60 AND 1h RSI > 55 AND 4h RSI < 72
2. PUMP WINDOW: 24h change in [4%, 20%]
3. BTC OK: BTC 24h > -2%
4. CATALYST: Any positive project-specific news OR strong technical pattern

=== OUTPUT REQUIREMENT ===
Output EXACTLY ONE JSON object. Do NOT output any text outside the JSON.
{{
  "rationale": "ONE short clause, max 12 words. No full sentences.",
  "steelman": "max 8 words, or empty",
  "direction": "BUY" or "WAIT",
  "confidence": "HIGH", "MEDIUM", or "LOW"
}}
"""


FUTURES_DEFAULT = """You are a SENIOR DERIVATIVES TRADER. You evaluate setups for {symbol} based on Technicals, Microstructure, and News.
Your default action is WAIT. You only deploy capital when the setup has multiple independent confirmations AND the asymmetry favors you.

=== MARKET DATA ===
Price: {price} | 24h: {change}% | Regime: {regime} | BTC 24h: {btc_24h}%
RSI: 15m={rsi_15m}, 1h={rsi_1h}, 4h={rsi_4h}
Derivatives: Funding={funding_rate:+.4f}% | OI Change={oi_change:+.1f}%
News: {news}

=== NEGATIVE CATALYST KEYWORDS ===
hack, exploit, breach, vulnerability, scam, fraud, rug, delist, lawsuit, SEC/CFTC, arrest, insolvency, depeg.
(Only counts if applied directly to {symbol} or its issuer. "Competitor hacked" is neutral.)

=== EVALUATION HEURISTICS ===
1. CONVERGENCE: Do the news align with the microstructure? (e.g., Bullish news + Rising OI = Strong LONG. Bearish news + Negative Funding = Squeeze risk, avoid SHORT).
2. FUNDING TRAPS: Funding > +0.08% means longs are crowded. Funding < -0.05% means squeeze risk.
3. EXHAUSTION: If 1h/4h RSI is > 70, LONGs are late. If < 30, SHORTs are late.
4. MACRO CONFLICT: If BTC is dumping (<-2%) but the altcoin is pumping, downgrade confidence.

=== SCREENER CONTEXT ===
{screener_context}

=== OUTPUT REQUIREMENT ===
Evaluate BOTH directions based on the heuristics. Output EXACTLY ONE JSON object.
Do NOT output any text outside the JSON.
{{
  "rationale": "ONE short clause, max 12 words. No full sentences.",
  "steelman": "max 8 words, or empty",
  "direction": "LONG", "SHORT", or "WAIT",
  "confidence": "HIGH", "MEDIUM", or "LOW"
}}
"""


#  Self-healing helper (called by launcher.pyw)
import os as _os  # noqa: E402 - kept beside the self-healing helper
import stat as _stat  # noqa: E402
import tempfile as _tempfile  # noqa: E402
from pathlib import Path as _Path  # noqa: E402

from bot_utils.atomic_publish import _sync_directory as _sync_prompt_directory  # noqa: E402


_PROMPT_PROJECT_ROOT = _Path(__file__).resolve().parent.parent


def _sync_prompt_parent_chain(directory: _Path) -> None:
    """Persist a newly created project-local prompt directory ancestry."""
    directory = directory.absolute()
    try:
        directory.relative_to(_PROMPT_PROJECT_ROOT)
    except ValueError:
        # Non-production callers still receive a useful leaf-parent barrier
        # without attempting to flush an unrelated filesystem root.
        anchor = directory.parent
    else:
        anchor = _PROMPT_PROJECT_ROOT
    current = directory
    while True:
        _sync_prompt_directory(current)
        if current == anchor:
            return
        parent = current.parent
        if parent == current:
            raise RuntimeError("prompt durability anchor is unreachable")
        current = parent


def _note_default_cleanup_error(
    primary: BaseException,
    cleanup_error: BaseException,
) -> None:
    try:
        primary.add_note(
            "default prompt temporary cleanup failed: "
            f"{type(cleanup_error).__name__}: {cleanup_error}"
        )
    except BaseException:
        pass


def _default_target_matches(path: str, raw: bytes) -> bool:
    try:
        current = _os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if (
        not _stat.S_ISREG(current.st_mode)
        or _os.path.islink(path)
        or current.st_size != len(raw)
    ):
        return False
    with open(path, "rb") as handle:
        return handle.read(len(raw) + 1) == raw


def _durable_create_default(path: str, content: str) -> bool:
    """Create one default without overwriting users or trusting stale temps."""
    raw = content.encode("utf-8")
    directory = _Path(_os.path.dirname(path) or ".").absolute()
    if _os.path.lexists(path):
        if _default_target_matches(path, raw):
            # A prior link may have become visible before its barrier failed.
            # Retrying the self-heal must be able to confirm that entry.
            _sync_prompt_directory(directory)
        return False

    fd, temporary = _tempfile.mkstemp(
        prefix=f".{_os.path.basename(path)}.",
        suffix=".tmp",
        dir=str(directory),
    )
    fd_owned = True
    temporary_owned = True
    temporary_identity: tuple[int, int] | None = None
    primary_error: BaseException | None = None
    try:
        temporary_stat = _os.fstat(fd)
        if not _stat.S_ISREG(temporary_stat.st_mode):
            raise OSError("default prompt temporary is not a regular file")
        temporary_identity = (
            temporary_stat.st_dev,
            temporary_stat.st_ino,
        )
        handle = None
        write_primary: BaseException | None = None
        try:
            handle = _os.fdopen(fd, "wb")
            fd_owned = False
            handle.write(raw)
            handle.flush()
            _os.fsync(handle.fileno())
        except BaseException as exc:
            write_primary = exc
            raise
        finally:
            if handle is not None:
                try:
                    handle.close()
                except BaseException as close_error:
                    if write_primary is None:
                        raise
                    _note_default_cleanup_error(write_primary, close_error)
        try:
            _os.link(temporary, path)
        except FileExistsError:
            if _default_target_matches(path, raw):
                _sync_prompt_directory(directory)
            return False
        _sync_prompt_directory(directory)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        if fd_owned:
            try:
                _os.close(fd)
            except BaseException as exc:
                cleanup_error = exc
        same_generation = False
        if temporary_owned and temporary_identity is not None:
            try:
                current = _os.stat(temporary, follow_symlinks=False)
                same_generation = (
                    _stat.S_ISREG(current.st_mode)
                    and not _os.path.islink(temporary)
                    and (current.st_dev, current.st_ino)
                    == temporary_identity
                )
            except FileNotFoundError:
                pass
            except BaseException as exc:
                cleanup_error = cleanup_error or exc
        if same_generation:
            try:
                _os.unlink(temporary)
                temporary_owned = False
                _sync_prompt_directory(directory)
            except FileNotFoundError:
                temporary_owned = False
            except BaseException as exc:
                cleanup_error = cleanup_error or exc
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            _note_default_cleanup_error(primary_error, cleanup_error)
    return True


def write_all_defaults(prompts_dir: str) -> list:
    """Write the default prompt files into `prompts_dir`.

    Only the LLM bots (Spot, Futures) have prompts. The Trend bot is
    purely mechanical and has no prompt.

    Called by launcher.pyw on first start (or after a user accidentally
    deletes the prompts folder) to restore working baseline prompts.

    Only writes files that DO NOT already exist -- never overwrites user
    edits. Returns a list of the filenames that were actually written
    (empty if everything was already in place).
    """
    try:
        _os.makedirs(prompts_dir, exist_ok=True)
        # Run on every invocation, not only immediately after mkdir: an
        # earlier process may have made directory entries visible but failed
        # before their parent barriers completed.
        _sync_prompt_parent_chain(_Path(prompts_dir))
    except Exception:
        return []
    targets = {
        "spot.txt": SPOT_DEFAULT,
        "spot_default.txt": SPOT_DEFAULT,
        "futures.txt": FUTURES_DEFAULT,
        "futures_default.txt": FUTURES_DEFAULT,
    }
    written = []
    for fname, content in targets.items():
        path = _os.path.join(prompts_dir, fname)
        try:
            if _durable_create_default(path, content):
                written.append(fname)
        except Exception:
            # Best-effort: skip files we cant write (permissions, disk full)
            # rather than raising up to the launcher.
            continue
    return written
