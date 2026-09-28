# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import socket
from contextlib import contextmanager
from unittest.mock import patch

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.x509 import load_pem_x509_certificate


def verify_certificate_matches_public_key(certificate: bytes, public_key: bytes) -> bool:
    """Return whether the given certificate corresponds to the given public key."""
    # Load certificate
    cert_pubkey = load_pem_x509_certificate(certificate, backend=default_backend()).public_key()

    # Load public key
    given_pubkey = serialization.load_pem_public_key(public_key, backend=default_backend())

    # Compare public keys as raw bytes
    cert_pubkey_bytes = cert_pubkey.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    given_pubkey_bytes = given_pubkey.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    return cert_pubkey_bytes == given_pubkey_bytes


@contextmanager
def mock_hostname_resolution(hostname: str, ip: str):
    original_getaddrinfo = socket.getaddrinfo

    def patched_getaddrinfo(host, *args, **kwargs):
        if host == hostname:
            return original_getaddrinfo(ip, *args, **kwargs)
        return original_getaddrinfo(host, *args, **kwargs)

    with patch("socket.getaddrinfo", side_effect=patched_getaddrinfo):
        yield
