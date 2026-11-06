# SPDX-License-Identifier: Apache-2.0
"""SCRAM-SHA-256 client (RFC 5802 / RFC 7677), sans-I/O: bytes in, bytes out."""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import stringprep
import unicodedata

from . import errors

MECHANISM = "SCRAM-SHA-256"
_MAX_ITERATIONS = 1 << 24  # refuse absurd server-chosen costs


def saslprep(password: str) -> str:
    """RFC 4013 SASLprep. Falls back to the raw password if it can't be prepared."""
    if password.isascii():
        return password
    try:
        mapped = "".join(
            " " if stringprep.in_table_c12(ch) else ch
            for ch in password
            if not stringprep.in_table_b1(ch)
        )
        out = unicodedata.normalize("NFKC", mapped)
        if not out:
            return password
        prohibited = (
            stringprep.in_table_c12, stringprep.in_table_c21_c22, stringprep.in_table_c3,
            stringprep.in_table_c4, stringprep.in_table_c5, stringprep.in_table_c6,
            stringprep.in_table_c7, stringprep.in_table_c8, stringprep.in_table_c9,
            stringprep.in_table_a1,
        )
        if any(check(ch) for ch in out for check in prohibited):
            return password
        if any(stringprep.in_table_d1(ch) for ch in out):
            if any(stringprep.in_table_d2(ch) for ch in out):
                return password
            if not (stringprep.in_table_d1(out[0]) and stringprep.in_table_d1(out[-1])):
                return password
        return out
    except Exception:  # pragma: no cover - defensive
        return password


def _attrs(message: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in message.split(","):
        key, sep, value = part.partition("=")
        if sep:
            out[key] = value
    return out


class ScramClient:
    def __init__(self, password: str, *, username: str = "", nonce: str | None = None) -> None:
        self._password = password
        self._nonce = nonce or base64.b64encode(os.urandom(18)).decode()
        self._first_bare = f"n={username},r={self._nonce}"
        self._server_signature = b""

    def client_first(self) -> bytes:
        return ("n,," + self._first_bare).encode()

    def client_final(self, server_first: bytes) -> bytes:
        text = server_first.decode()
        attrs = _attrs(text)
        if "e" in attrs:
            raise errors.OperationalError(f"SCRAM authentication failed: {attrs['e']}")
        try:
            server_nonce, salt, iterations = attrs["r"], base64.b64decode(attrs["s"]), int(attrs["i"])
        except (KeyError, ValueError) as exc:
            raise errors.OperationalError("malformed SCRAM server-first-message") from exc
        if not server_nonce.startswith(self._nonce):
            raise errors.OperationalError("SCRAM server nonce does not extend the client nonce")
        if not 1 <= iterations <= _MAX_ITERATIONS:
            raise errors.OperationalError(f"unacceptable SCRAM iteration count {iterations}")
        salted = hashlib.pbkdf2_hmac("sha256", saslprep(self._password).encode(), salt, iterations)
        client_key = hmac.digest(salted, b"Client Key", "sha256")
        stored_key = hashlib.sha256(client_key).digest()
        without_proof = f"c=biws,r={server_nonce}"
        auth_message = f"{self._first_bare},{text},{without_proof}".encode()
        signature = hmac.digest(stored_key, auth_message, "sha256")
        proof = bytes(a ^ b for a, b in zip(client_key, signature))
        server_key = hmac.digest(salted, b"Server Key", "sha256")
        self._server_signature = hmac.digest(server_key, auth_message, "sha256")
        return f"{without_proof},p={base64.b64encode(proof).decode()}".encode()

    def verify_server_final(self, server_final: bytes) -> None:
        attrs = _attrs(server_final.decode())
        if "e" in attrs:
            raise errors.OperationalError(f"SCRAM authentication failed: {attrs['e']}")
        try:
            received = base64.b64decode(attrs["v"])
        except (KeyError, ValueError) as exc:
            raise errors.OperationalError("malformed SCRAM server-final-message") from exc
        if not self._server_signature or not received:
            raise errors.OperationalError("SCRAM server signature is missing: the server could not be verified")
        if not hmac.compare_digest(received, self._server_signature):
            raise errors.OperationalError("SCRAM server signature does not match: the server could not be verified")
