"""Portable SSL context using certifi's CA bundle.

The python.org macOS installers ship without a usable system trust store, so
outbound HTTPS/WSS from aiohttp and websockets fails by default. Passing a
certifi-backed SSLContext explicitly sidesteps the platform issue and is a
no-op on Linux / cloud runners where the system store works fine.
"""

from __future__ import annotations

import ssl

import certifi

_CONTEXT: ssl.SSLContext | None = None


def ssl_context() -> ssl.SSLContext:
    """Return a cached, certifi-backed SSLContext."""
    global _CONTEXT
    if _CONTEXT is None:
        _CONTEXT = ssl.create_default_context(cafile=certifi.where())
    return _CONTEXT
