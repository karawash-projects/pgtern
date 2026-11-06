# SPDX-License-Identifier: Apache-2.0
"""The client side of the authentication exchange, sans-I/O: messages in, replies out.

The blocking and the asyncio clients both feed every Authentication message to
:meth:`Authenticator.step` and send what it returns. The rules here decide what the client
agrees to: a password only goes over an encrypted connection (or a Unix socket), SASL steps
must arrive in order, and a SCRAM login is accepted only once the server has proven it knows
the password.
"""
from __future__ import annotations

import hashlib

from . import _protocol as P
from . import errors
from ._scram import MECHANISM, MECHANISM_PLUS, ScramClient

_OK = 0
_CLEARTEXT = 3
_MD5 = 5
_SASL = 10
_SASL_CONTINUE = 11
_SASL_FINAL = 12


class Authenticator:
    def __init__(self, user: str, password: str | None, *, encrypted: bool,
                 allow_plaintext: bool = False, require_scram: bool = False,
                 channel_binding: bytes | None = None) -> None:
        self.mechanism: str | None = None  # the SASL mechanism used, once one is chosen
        self._channel_binding = channel_binding  # tls-server-end-point data when the connection is TLS
        self._user = user
        self._password = password
        self._encrypted = encrypted
        self._allow_plaintext = allow_plaintext  # sslmode=disable: the caller asked for no encryption
        self._require_scram = require_scram
        self._scram: ScramClient | None = None
        self._scram_step = 0  # 0 not started, 1 initial sent, 2 final sent, 3 server proof verified
        self._done = False    # AuthenticationOk received

    def _secret(self) -> str:
        if self._password is None:
            raise errors.OperationalError("the server requested a password, but none was provided")
        return self._password

    def _require_encryption(self, what: str) -> None:
        if not self._encrypted and not self._allow_plaintext:
            raise errors.OperationalError(
                f"the server asked for {what} on an unencrypted connection; refusing to send the password "
                "(use sslmode=disable only if you accept plaintext)")

    def step(self, code: int, body: bytes) -> bytes:
        """Handle one Authentication message (``body`` is the payload after the 4-byte code).

        Returns the message to send back, or ``b""`` when nothing needs to be sent.
        """
        if self._done:
            raise errors.OperationalError("the server sent another authentication request after accepting the login")
        if self._require_scram and code in (_OK, _CLEARTEXT, _MD5):
            raise errors.OperationalError("the server did not use SCRAM-SHA-256, as require_auth requires")

        if code == _OK:
            if self._scram is not None and self._scram_step != 3:
                raise errors.OperationalError(
                    "the server accepted the login without proving it knows the password")
            self._done = True
            return b""

        if code == _CLEARTEXT:
            self._require_encryption("a cleartext password")
            return P.password_message(self._secret().encode())

        if code == _MD5:
            self._require_encryption("an md5-hashed password")
            salt = body[:4]
            inner = hashlib.md5((self._secret() + self._user).encode(), usedforsecurity=False).hexdigest()
            outer = hashlib.md5(inner.encode() + salt, usedforsecurity=False).hexdigest()
            return P.password_message(b"md5" + outer.encode())

        if code == _SASL:
            if self._scram is not None:
                raise errors.OperationalError("the server started a second SASL exchange")
            mechanisms = [m for m in body.split(b"\0") if m]
            if self._channel_binding is not None and MECHANISM_PLUS.encode() in mechanisms:
                # over TLS, bind the login to this connection: a man in the middle cannot replay it
                mechanism, binding = MECHANISM_PLUS, self._channel_binding
            elif MECHANISM.encode() in mechanisms:
                mechanism, binding = MECHANISM, None
            else:
                raise errors.NotSupportedError(
                    "the server offers no supported SASL mechanism: " + ", ".join(m.decode() for m in mechanisms))
            self.mechanism = mechanism
            self._scram = ScramClient(self._secret(), channel_binding=binding)
            self._scram_step = 1
            return P.sasl_initial_response(mechanism, self._scram.client_first())

        if code == _SASL_CONTINUE:
            if self._scram is None or self._scram_step != 1:
                raise errors.OperationalError("SASL continue message out of order")
            self._scram_step = 2
            return P.sasl_response(self._scram.client_final(body))

        if code == _SASL_FINAL:
            if self._scram is None or self._scram_step != 2:
                raise errors.OperationalError("SASL final message out of order: the server skipped its proof")
            self._scram.verify_server_final(body)
            self._scram_step = 3
            return b""

        raise errors.NotSupportedError(f"authentication method {code} is not supported")


# ---------------------------------------------------------------- channel binding data (RFC 5929)
# tls-server-end-point binds to the hash of the server certificate, using the hash its signature
# algorithm names (MD5 and SHA-1 are replaced by SHA-256). The certificate is parsed here, with no
# dependency, just far enough to read that OID.

_OID_HASH = {
    "1.2.840.113549.1.1.4": "sha256", "1.2.840.113549.1.1.5": "sha256",  # md5/sha1 with RSA -> sha256
    "1.2.840.113549.1.1.11": "sha256", "1.2.840.113549.1.1.12": "sha384",
    "1.2.840.113549.1.1.13": "sha512", "1.2.840.113549.1.1.14": "sha224",
    "1.2.840.10045.4.1": "sha256", "1.2.840.10045.4.3.2": "sha256",  # ecdsa with sha1 / sha256
    "1.2.840.10045.4.3.3": "sha384", "1.2.840.10045.4.3.4": "sha512",
}
_OID_PSS = "1.2.840.113549.1.1.10"


def _tlv(buf: bytes, pos: int) -> tuple[int, int, int]:
    """Tag, value start and value end of the DER element at ``pos``."""
    if pos + 2 > len(buf):
        raise errors.OperationalError("malformed server certificate")
    length, start = buf[pos + 1], pos + 2
    if length & 0x80:
        count = length & 0x7F
        if count == 0 or count > 4 or start + count > len(buf):
            raise errors.OperationalError("malformed server certificate")
        length = int.from_bytes(buf[start:start + count], "big")
        start += count
    end = start + length
    if end > len(buf):
        raise errors.OperationalError("malformed server certificate")
    return buf[pos], start, end


def _oid_text(value: bytes) -> str:
    arcs: list[int] = []
    number = 0
    for byte in value:
        number = (number << 7) | (byte & 0x7F)
        if not byte & 0x80:
            arcs.append(number)
            number = 0
    first = arcs[0] if arcs else 0
    head = [first // 40, first % 40] if first < 80 else [2, first - 80]
    return ".".join(str(a) for a in head + arcs[1:])


def signature_hash(der: bytes) -> str:
    """The hash name the certificate's signature algorithm uses (RFC 5929: MD5 and SHA-1 become SHA-256)."""
    _, cert_start, _ = _tlv(der, 0)                      # Certificate ::= SEQUENCE
    _, _, tbs_end = _tlv(der, cert_start)                # tbsCertificate, skipped
    _, alg_start, alg_end = _tlv(der, tbs_end)           # signatureAlgorithm
    _, oid_start, oid_end = _tlv(der, alg_start)
    oid = _oid_text(der[oid_start:oid_end])
    if oid == _OID_PSS:  # the hash is named in the parameters; the default is SHA-1, which maps to SHA-256
        if oid_end < alg_end:
            _, params_start, params_end = _tlv(der, oid_end)
            if params_start < params_end and der[params_start] == 0xA0:  # [0] hashAlgorithm
                _, inner_start, _ = _tlv(der, params_start)
                _, alg_id_start, _ = _tlv(der, inner_start)
                _, hash_start, hash_end = _tlv(der, alg_id_start)
                hash_oid = _oid_text(der[hash_start:hash_end])
                return {"2.16.840.1.101.3.4.2.1": "sha256", "2.16.840.1.101.3.4.2.2": "sha384",
                        "2.16.840.1.101.3.4.2.3": "sha512", "2.16.840.1.101.3.4.2.4": "sha224"}.get(hash_oid, "sha256")
        return "sha256"
    return _OID_HASH.get(oid, "sha256")


def channel_binding_data(tls: object) -> bytes | None:
    """The tls-server-end-point binding for an established TLS socket or SSL object, or None."""
    getpeercert = getattr(tls, "getpeercert", None)
    der = getpeercert(binary_form=True) if getpeercert is not None else None
    if not der:
        return None
    return hashlib.new(signature_hash(der), der).digest()
