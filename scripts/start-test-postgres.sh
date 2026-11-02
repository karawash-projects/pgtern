#!/usr/bin/env bash
# Start a throwaway PostgreSQL for the integration tests: SCRAM, MD5 and cleartext
# users, plus TLS with a self-signed certificate. Prints the environment to export.
#
#   eval "$(scripts/start-test-postgres.sh)"
#   pytest
#   scripts/start-test-postgres.sh stop
#
# Needs the PostgreSQL server binaries and openssl. When run as root it runs the
# server as the `postgres` user.
set -euo pipefail

PGBIN="${PGBIN:-$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)}"
PORT="${PGTERN_PORT:-54329}"
BASE="${PGTERN_PGROOT:-/var/lib/postgresql/pgtern-test}"
DATA="$BASE/data"
RUN=""
if [ "$(id -u)" = 0 ]; then RUN="runuser -u postgres --"; fi

if [ "${1:-}" = "stop" ]; then
  $RUN "$PGBIN/pg_ctl" -D "$DATA" stop -m fast >/dev/null 2>&1 || true
  exit 0
fi

if [ ! -d "$DATA" ]; then
  mkdir -p "$BASE"
  [ "$(id -u)" = 0 ] && chown postgres:postgres "$BASE"
  $RUN bash -c "echo postgres > '$BASE/pw' && '$PGBIN/initdb' -D '$DATA' -U postgres -E UTF8 --locale=C.UTF-8 --pwfile='$BASE/pw' --auth-host=scram-sha-256 --auth-local=trust >/dev/null"
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout "$BASE/server.key" -out "$BASE/server.crt" 2>/dev/null
  [ "$(id -u)" = 0 ] && chown postgres:postgres "$BASE/server.key" "$BASE/server.crt"
  chmod 600 "$BASE/server.key"
  {
    echo "host all md5user 127.0.0.1/32 md5"
    echo "host all clearuser 127.0.0.1/32 password"
    echo "host all trustuser 127.0.0.1/32 trust"
    echo "hostssl all tlsuser 127.0.0.1/32 scram-sha-256"
    echo "hostnossl all tlsuser 127.0.0.1/32 reject"
    cat "$DATA/pg_hba.conf"
  } > "$BASE/hba.new"
  cp "$BASE/hba.new" "$DATA/pg_hba.conf"
  [ "$(id -u)" = 0 ] && chown postgres:postgres "$DATA/pg_hba.conf"
fi

if ! $RUN "$PGBIN/pg_isready" -h 127.0.0.1 -p "$PORT" >/dev/null 2>&1; then
  $RUN "$PGBIN/pg_ctl" -D "$DATA" -l "$BASE/server.log" -w start \
    -o "-p $PORT -c listen_addresses=127.0.0.1,127.0.0.2 -c unix_socket_directories=$BASE -c ssl=on \
-c ssl_cert_file=$BASE/server.crt -c ssl_key_file=$BASE/server.key -c password_encryption=scram-sha-256 \
-c max_connections=100" >/dev/null
  PSQL="$PGBIN/psql -h $BASE -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1 -q"
  $RUN $PSQL -c "SET password_encryption='md5'; CREATE ROLE md5user LOGIN PASSWORD 'md5pw';" \
             -c "SET password_encryption='scram-sha-256'; CREATE ROLE clearuser LOGIN PASSWORD 'clearpw'; CREATE ROLE tlsuser LOGIN PASSWORD 'tlspw'; CREATE ROLE trustuser LOGIN;" \
             >/dev/null 2>&1 || true
fi

cat <<EOF
export PGTERN_TEST_DSN="postgresql://postgres:postgres@127.0.0.1:$PORT/postgres"
export PGTERN_TEST_HOST=127.0.0.1
export PGTERN_TEST_PORT=$PORT
export PGTERN_TEST_SOCKET_DIR=$BASE
export PGTERN_TEST_CA=$BASE/server.crt
EOF
