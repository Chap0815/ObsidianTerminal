"""
prompts_defaults.py  Embedded Default-Prompts as Fallback

    Self-healing: on first launcher start the launcher writes these into
    prompts/*.txt and prompts/*_default.txt so the user always has a working
    editable prompt plus a baseline fallback.

Kept in sync with the files in prompts/.
"""

SPOT_DEFAULT = """You are a MOMENTUM SPECIALIST evaluating SPOT entries for {symbol} to capture strong directional moves.

Your bias is to ENGAGE when confirmations align. Choose BUY when the momentum thesis is supported by >=2 signals and no hard blocker triggers. Choose WAIT only when signals contradict, are missing, or a hard blocker fires  -  NOT as a default.

=== MARKET DATA ===
Symbol: {symbol} | 24h: {change}% | Regime: {regime}
BTC 24h: {btc_24h:+.2f}% | BTC 7d: {btc_7d:+.2f}% | F&G: {fg} ({fg_label})
RSI: 15m={rsi_15m:.1f}, 1h={rsi_1h:.1f}, 4h={rsi_4h:.1f}
News: {news}

=== HARD BLOCKERS (auto-WAIT) ===
Trigger WAIT immediately if ANY apply:
- Direct negative catalyst on {symbol}: hack, exploit, breach, scam, fraud, rug, delist, ban, lawsuit, SEC/CFTC, arrest, bankruptcy, depeg, insider sell.
- 24h change > +25% (parabolic, late entry).
- BTC 24h < -3% (broad market headwind too strong for momentum longs).
- F&G >= 85 (extreme greed, top risk).
- 4h RSI > 78 (severely overbought).

=== BUY SIGNALS (vote BUY when >=2 align) ===
- IMPULSE: 15m RSI > 60 AND 1h RSI > 55 AND 4h RSI < 72 (clean impulse with room).
- PUMP WINDOW: 24h change in [+4%, +20%] (valid momentum range).
- BTC OK: BTC 24h > -2% (no significant macro drag).
- CATALYST: Positive project-specific news or event tied to {symbol}.

=== BEAR REGIME ADJUSTMENT ===
In BEAR regime, require EITHER a clear positive catalyst OR exceptional momentum
(15m RSI > 65 AND 24h change > +6%). Otherwise WAIT.

=== CONFIDENCE GUIDELINES ===
- HIGH: 3+ BUY signals aligned, clean BTC context, no overbought warning.
- MEDIUM: 2 BUY signals aligned, minor friction tolerable.
- LOW: 2 BUY signals but with mixed BTC or stretched RSI.

=== OUTPUT REQUIREMENT ===
Output EXACTLY ONE JSON object. Do NOT output any text outside the JSON.
Allowed values: direction = BUY or WAIT. confidence = HIGH, MEDIUM, or LOW.

{
  "rationale": "ONE short clause, max 12 words. No full sentences.",
  "steelman": "max 8 words, or empty",
  "direction": "BUY",
  "confidence": "HIGH"
}
"""


FUTURES_DEFAULT = """You are a SENIOR DERIVATIVES TRADER evaluating {symbol} for a leveraged perpetual setup based on Technicals, Microstructure, and News.

Your task is to identify setups where evidence clearly favors one side. Choose LONG or SHORT when the signals align. Use WAIT only when signals genuinely contradict, are absent, or a hard blocker triggers  -  NOT as a safe default.

=== MARKET DATA ===
Price: {price} | 24h: {change}% | Regime: {regime} | BTC 24h: {btc_24h}%
RSI: 15m={rsi_15m}, 1h={rsi_1h}, 4h={rsi_4h}
Derivatives: Funding={funding_rate}% | OI Change={oi_change}%
News: {news}

=== SCREENER CONTEXT ===
{screener_context}

=== HARD BLOCKERS (auto-WAIT) ===
Trigger WAIT immediately if ANY apply:
- Direct negative catalyst on {symbol}: hack, exploit, breach, vulnerability, scam, fraud, rug, delist, lawsuit, SEC/CFTC, arrest, insolvency, depeg. ("Competitor hacked" does NOT count.)
- 24h change > +25% AND 1h RSI > 75 (parabolic, late chase).
- Funding > +0.15% (dangerously crowded longs  -  chase risk).
- Funding < -0.10% (capitulation, two-sided squeeze risk).

=== BEAR-MARKET BIAS GUARD (read carefully) ===
The "extreme fear = contrarian buying opportunity" heuristic is OFTEN WRONG. Crypto can fall further from extreme-fear levels for days or weeks. Do NOT treat a low FEAR & GREED reading as a bullish signal on its own.

When the regime is BEAR or NEUTRAL with macro headwind (BTC 24h <= -1%):
- A low FEAR & GREED reading is NOT bullish evidence. Ignore it for direction.
- LONG confidence MUST NOT be HIGH unless ALL of these hold:
  - 3+ explicit LONG signals from the list below align, AND
  - There is a direct positive catalyst on {symbol} itself (not market-wide), AND
  - 4h RSI > 50 (the asset itself is in uptrend, not just bouncing).
- Otherwise: LONG confidence MAY be MEDIUM, never HIGH.
- "Bullish technicals + extreme fear" by itself is NOT enough for HIGH  -  it is the exact reasoning that loses money in falling markets.

=== LONG SIGNALS (vote LONG when >=2 align) ===
- Screener flagged candidate as LONG (MACD > 0, momentum confirmed).
- 24h change in [+2%, +20%] with 4h RSI < 70.
- Bullish news or catalyst tied directly to {symbol}.
- Funding in [-0.02%, +0.06%] AND OI change > +5% (clean accumulation).
- BTC 24h >= -1% (no macro headwind).

=== SHORT SIGNALS (vote SHORT when >=2 align) ===
- Screener flagged candidate as SHORT (MACD < 0, weakness confirmed).
- 24h change in [-20%, -2%] with 4h RSI > 30.
- Bearish news or catalyst tied directly to {symbol}.
- Funding > +0.08% (crowded longs, squeeze setup) OR OI change < -5% (positions unwinding).
- BTC 24h <= +1% (no strong bull macro fighting the short).

=== CONFIDENCE GUIDELINES ===
- HIGH: 3+ signals aligned, no contradictions, clean macro context. See BEAR-MARKET BIAS GUARD above for additional restrictions on LONG HIGH in bear conditions.
- MEDIUM: 2 signals aligned, minor contradictions tolerable.
- LOW: 2 signals aligned but with notable contradictions or weak data.

=== OUTPUT REQUIREMENT ===
Output EXACTLY ONE JSON object. Do NOT output any text outside the JSON.
Allowed values: direction = LONG, SHORT, or WAIT. confidence = HIGH, MEDIUM, or LOW.

{
  "rationale": "ONE short clause, max 12 words. No full sentences.",
  "steelman": "max 8 words, or empty",
  "direction": "LONG",
  "confidence": "HIGH"
}"""


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
