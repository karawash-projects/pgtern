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
from ._scram import MECHANISM, ScramClient

_OK = 0
_CLEARTEXT = 3
_MD5 = 5
_SASL = 10
_SASL_CONTINUE = 11
_SASL_FINAL = 12


class Authenticator:
    def __init__(self, user: str, password: str | None, *, encrypted: bool,
                 allow_plaintext: bool = False, require_scram: bool = False) -> None:
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
            if MECHANISM.encode() not in mechanisms:
                raise errors.NotSupportedError(
                    "the server offers no supported SASL mechanism: " + ", ".join(m.decode() for m in mechanisms))
            self._scram = ScramClient(self._secret())
            self._scram_step = 1
            return P.sasl_initial_response(MECHANISM, self._scram.client_first())

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
