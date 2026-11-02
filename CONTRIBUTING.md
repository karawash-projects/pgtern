# Contributing to pgtern

Thank you for considering a contribution. pgtern is a small library with a strict scope:
zero runtime dependencies, standard library only, and a stable public API. Please keep
these in mind when you propose a change.

## Getting started

```
git clone <repository-url> pgtern
cd pgtern
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
```

## Running the tests

The unit tests need no server:

```
pytest tests/test_unit.py
```

The full suite needs a PostgreSQL server. The helper script starts a throwaway cluster with
SCRAM, MD5, cleartext, trust and TLS users:

```
eval "$(scripts/start-test-postgres.sh)"
pytest
scripts/start-test-postgres.sh stop
```

To test another server, set `PGTERN_TEST_DSN` (and `PGTERN_TEST_HOST`, `PGTERN_TEST_PORT`
for the tests that open their own connections). Every change must pass the suite against
all supported PostgreSQL major versions.

## Guidelines

- **No new runtime dependencies.** pgtern imports only the standard library. Test and build
  tooling belongs in the `dev` extra.
- **Keep the public API stable.** Anything exported from `pgtern` or its submodules is public.
  Changes that break it need a major version and a changelog entry.
- **Protocol changes need tests against a real server.** Mock tests cannot show that the
  wire protocol is right.
- **Security-sensitive code** (authentication, TLS, parameter encoding) needs tests for the
  failure paths as well as the success paths.
- **Style:** type-annotate public functions, keep modules focused, and write docstrings for
  public behaviour.
- **Changelog:** add an entry under `[Unreleased]` in `CHANGELOG.md` for every user-visible
  change.

## Submitting a change

1. Open an issue first for anything larger than a bug fix, so the design can be agreed early.
2. Make focused commits with clear messages that explain the why.
3. Run the full test suite against at least PostgreSQL 14 and the newest supported version.
4. Open a pull request describing the change, how it was tested, and any compatibility impact.

By contributing, you agree that your contribution is licensed under the Apache License 2.0.

## Reporting security issues

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md).
