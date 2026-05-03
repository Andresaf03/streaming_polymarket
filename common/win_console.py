"""Small Windows console compatibility helpers."""

from __future__ import annotations

import os
import sys


def configure_utf8_console() -> None:
    """Prefer UTF-8 output on Windows terminals.

    PowerShell may expose stdout/stderr as cp1252, which cannot encode symbols
    used in Rich status lines. Reconfigure when supported and fall back to
    replacement instead of crashing a long-running stream.
    """
    if os.name != "nt":
        return

    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            pass
