"""Internal AcornOps transport TLS helpers."""

from __future__ import annotations

import ssl
from typing import Any

from execution_engine.config import settings


def httpx_tls_kwargs() -> dict[str, Any]:
    """Return httpx TLS kwargs for AcornOps internal service calls."""
    if not settings.INTERNAL_TRANSPORT_TLS_ENABLED:
        return {}
    kwargs: dict[str, Any] = {
        "verify": settings.INTERNAL_TRANSPORT_TLS_CA_FILE,
    }
    if settings.INTERNAL_TRANSPORT_TLS_REQUIRE_CLIENT_CERT:
        kwargs["cert"] = (
            settings.INTERNAL_TRANSPORT_TLS_CERT_FILE,
            settings.INTERNAL_TRANSPORT_TLS_KEY_FILE,
        )
    return kwargs


def uvicorn_ssl_kwargs() -> dict[str, Any]:
    """Return Uvicorn SSL kwargs for the business listener."""
    if not settings.INTERNAL_TRANSPORT_TLS_ENABLED:
        return {}
    kwargs: dict[str, Any] = {
        "ssl_certfile": settings.INTERNAL_TRANSPORT_TLS_CERT_FILE,
        "ssl_keyfile": settings.INTERNAL_TRANSPORT_TLS_KEY_FILE,
    }
    if settings.INTERNAL_TRANSPORT_TLS_CA_FILE:
        kwargs["ssl_ca_certs"] = settings.INTERNAL_TRANSPORT_TLS_CA_FILE
    if settings.INTERNAL_TRANSPORT_TLS_REQUIRE_CLIENT_CERT:
        kwargs["ssl_cert_reqs"] = ssl.CERT_REQUIRED
    return kwargs
