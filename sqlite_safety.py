"""Connection-local SQLite safeguards; safe to import from recovery tools."""
from __future__ import annotations

import sqlite3


def harden_sqlite_connection(
    connection: sqlite3.Connection, *, close_on_failure: bool = True
) -> sqlite3.Connection:
    """Harden a newly opened handle before SQL, or close it and fail closed.

    These settings reduce unsafe SQL/schema capabilities. They do not patch
    SQLite itself or make an attacker-supplied database safe to open. Existing
    lifecycle owners may retain cleanup responsibility with close_on_failure=False.
    """
    try:
        for option, expected in (
            (sqlite3.SQLITE_DBCONFIG_DEFENSIVE, True),
            (sqlite3.SQLITE_DBCONFIG_TRUSTED_SCHEMA, False),
        ):
            connection.setconfig(option, expected)
            if connection.getconfig(option) is not expected:
                raise sqlite3.OperationalError("SQLite connection safety setting was not applied")
    except BaseException as setup_error:
        if not close_on_failure:
            raise
        try:
            connection.close()
        except BaseException as close_error:
            try:
                setup_error.add_note(
                    "close SQLite handle after safety setup failure: "
                    f"{type(close_error).__name__}"
                )
            except BaseException:
                # Even a failing exception annotation must preserve setup_error.
                pass
        raise
    return connection
