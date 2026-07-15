"""Additive trust configuration for outbound TLS clients."""

from __future__ import annotations

import ssl
from typing import Any
from urllib.parse import urlparse

import httpx

from execution_engine.config import settings


def additional_ca_httpx_ssl_context() -> ssl.SSLContext:
    """Return HTTPX's normal trust extended with the operator CA bundle."""
    context = httpx.create_ssl_context()
    if settings.ADDITIONAL_CA_BUNDLE_FILE:
        context.load_verify_locations(cafile=settings.ADDITIONAL_CA_BUNDLE_FILE)
    return context


def internal_httpx_ssl_context(*ca_files: str | None) -> ssl.SSLContext:
    """Return verified trust limited to explicit internal/operator CA bundles."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    for ca_file in ca_files:
        if ca_file:
            context.load_verify_locations(cafile=ca_file)
    return context


def redis_tls_kwargs(redis_url: str) -> dict[str, Any]:
    """Apply the additional CA only when the operator selected Redis TLS."""
    if urlparse(redis_url).scheme != "rediss" or not settings.ADDITIONAL_CA_BUNDLE_FILE:
        return {}
    return {
        "ssl_ca_certs": settings.ADDITIONAL_CA_BUNDLE_FILE,
        "ssl_cert_reqs": "required",
        "ssl_check_hostname": True,
    }
