# SPDX-License-Identifier: Apache-2.0
"""SCRAM-SHA-256-PLUS: the login is bound to the TLS connection (tls-server-end-point)."""
from __future__ import annotations

import hashlib
import ssl

import pytest

import pgtern
from pgtern import _auth, _scram
from conftest import CA, url

SHA384_RSA = b"\x2a\x86\x48\x86\xf7\x0d\x01\x01\x0c"   # 1.2.840.113549.1.1.12
SHA1_RSA = b"\x2a\x86\x48\x86\xf7\x0d\x01\x01\x05"     # 1.2.840.113549.1.1.5
PSS = b"\x2a\x86\x48\x86\xf7\x0d\x01\x01\x0a"          # 1.2.840.113549.1.1.10
SHA512 = b"\x60\x86\x48\x01\x65\x03\x04\x02\x03"       # 2.16.840.1.101.3.4.2.3


def _der(tag: int, body: bytes) -> bytes:
    assert len(body) < 128
    return bytes([tag, len(body)]) + body


def _cert(sig_alg_params: bytes) -> bytes:
    """A certificate skeleton: only the structure the binding reader looks at is meaningful."""
    return _der(0x30, _der(0x30, b"") + sig_alg_params + _der(0x03, b"\x00"))


def _alg(oid: bytes, params: bytes = b"") -> bytes:
    return _der(0x30, _der(0x06, oid) + params)


def test_signature_hash_reads_the_signature_algorithm():
    assert _auth.signature_hash(_cert(_alg(SHA384_RSA))) == "sha384"


def test_md5_and_sha1_signatures_bind_with_sha256():
    assert _auth.signature_hash(_cert(_alg(SHA1_RSA))) == "sha256"


def test_pss_names_its_hash_in_the_parameters():
    params = _der(0x30, _der(0xA0, _der(0x30, _der(0x06, SHA512))))  # hashAlgorithm [0] SHA-512
    assert _auth.signature_hash(_cert(_alg(PSS, params))) == "sha512"


def test_pss_without_parameters_defaults_to_sha256():
    assert _auth.signature_hash(_cert(_alg(PSS))) == "sha256"


def test_binding_data_is_the_hash_of_the_certificate():
    class FakeTls:
        def getpeercert(self, binary_form: bool):
            assert binary_form
            return _cert(_alg(SHA384_RSA))

    der = _cert(_alg(SHA384_RSA))
    assert _auth.channel_binding_data(FakeTls()) == hashlib.sha384(der).digest()
    assert _auth.channel_binding_data(object()) is None


def test_authenticator_picks_plus_over_tls_and_plain_otherwise():
    offer = b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0"
    over_tls = _auth.Authenticator("u", "pw", encrypted=True, channel_binding=b"\x01" * 32)
    over_tls.step(10, offer)
    assert over_tls.mechanism == "SCRAM-SHA-256-PLUS"

    plain = _auth.Authenticator("u", "pw", encrypted=True, channel_binding=None)
    plain.step(10, offer)
    assert plain.mechanism == "SCRAM-SHA-256"


def test_plus_client_first_announces_the_binding_type():
    client = _scram.ScramClient("pw", username="u", nonce="abcdef", channel_binding=b"\x02" * 32)
    assert client.client_first().startswith(b"p=tls-server-end-point,,n=u,r=abcdef")
    plain = _scram.ScramClient("pw", username="u", nonce="abcdef")
    assert plain.client_first().startswith(b"n,,n=u,r=abcdef")


@pytest.mark.integration
@pytest.mark.skipif(not CA, reason="needs the test server certificate")
def test_test_server_certificate_binds_with_sha256():
    with open(CA) as handle:
        der = ssl.PEM_cert_to_DER_cert(handle.read())
    assert _auth.signature_hash(der) == "sha256"


@pytest.mark.auth_roles
def test_login_over_tls_uses_plus_and_the_server_accepts_it():
    # tlsuser may only log in over TLS with SCRAM: the server checks our binding for the PLUS mechanism
    with pgtern.connect(url("tlsuser", "tlspw", sslmode="require")) as conn:
        assert conn.execute("SELECT current_user").fetchone() == ("tlsuser",)
        assert conn._auth is not None and conn._auth.mechanism == "SCRAM-SHA-256-PLUS"


@pytest.mark.auth_roles
def test_async_login_over_tls_uses_plus():
    import asyncio
    from pgtern import aio

    async def main():
        async with await aio.connect(url("tlsuser", "tlspw", sslmode="require")) as conn:
            row = (await conn.execute("SELECT current_user")).fetchone()
            return row, conn._auth.mechanism

    row, mechanism = asyncio.run(asyncio.wait_for(main(), 60))
    assert row == ("tlsuser",)
    assert mechanism == "SCRAM-SHA-256-PLUS"
