# Changelog

All notable changes to pgtern are documented in this file.

## [Unreleased]

## [0.4.1] - 2026-11-06

### Fixed

- The notifications example in the README connects with `autocommit=True`, so `LISTEN` takes effect.
- The package index now shows project links (homepage, source, issues and changelog).

### Documentation

- The README follows the usual project layout, with absolute links and a status note.
- The security support table lists 0.4.x as supported.
- The contributing guide has the real clone URL.

## [0.4.0] - 2026-11-06

### Changed (behaviour)

These changes can affect existing code. Check them when upgrading.

- `timestamptz` values are returned in UTC in text mode as well as binary mode, so both formats give the same value and `tzinfo`.
- `connect_timeout` defaults to 30 seconds and applies to each host attempt. `connect_timeout=0` means no limit.
- `prepare=True` raises `NotSupportedError`. `prepare=False` is accepted.
- Bare `%b` and `%t` placeholders are refused with `ProgrammingError`. Only `%s` is accepted.
- A statement with more placeholders than parameters raises `ProgrammingError` instead of `IndexError`.
- Non-ASCII SQL or parameters under a non-UTF-8 `client_encoding` raise `NotSupportedError` before anything is sent.
- Using a connection from inside a COPY block or from inside a notice or notify handler raises `ProgrammingError` at once, instead of breaking the protocol or deadlocking.
- A query hook that fails on entry or exit is logged, not raised. A notice or notify handler that raises is logged too.

### Added

- SCRAM-SHA-256-PLUS: when the server offers it over TLS, the login is bound to that connection (`tls-server-end-point`, RFC 5929).
- `max_result_bytes` connection attribute, a cap on the bytes one statement may return. It is `None` (no cap) by default. A result over the cap raises `DataError` and leaves the connection usable.
- The async pool resets itself after `fork()`, so a forked child never uses the parent's connections.

### Fixed

- `24:00:00` is refused with `DataError` in binary mode. It used to be read as midnight.
- A `float4` parameter outside its range is sent as text, so the server reports the error instead of the driver raising it.
- Python `bool` values are no longer captured by a dumper registered for `int`.
- Session settings are followed after `SET client_encoding` and `SET DateStyle`, both when reading rows and when copying data.
- A failed connection open no longer leaves sessions behind.
- Array element delimiters come from the type, so arrays of `box` split correctly on `;`.
- The SCRAM client rejects a server nonce that does not extend its own, and rejects malformed base64.
- Bytes literals are correct when `standard_conforming_strings` is off.
- The type-loader cache is cleared after DDL, so a changed type is read again.
- Connect timeouts are enforced for the whole startup, so a silent server cannot hang a connection.

### Infrastructure

- Continuous integration: lint and type check, a test matrix over Python 3.11 to 3.13 and PostgreSQL 14 to 18, a TLS, auth-roles, pooler and restart job, an installed-wheel test in a clean environment, and a build job that checks metadata and runtime dependencies.
- Development status classifier set to Beta.

## [0.3.0]

Earlier release. Notes for this version were not kept in this file.
