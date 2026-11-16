#!/bin/bash
# Bootstrap the two databases and the two roles, on FIRST start only.
#
# This runs from the postgres image's `docker-entrypoint-initdb.d`, which means it
# runs exactly once, against an empty data directory. To re-run it:
#     docker compose down -v      # ⚠ DESTROYS the archive and the linked session
#
# 🔴 THE SPLIT HERE IS A SECURITY PROPERTY, NOT TIDINESS.
# `whatsapp_app` is the role the CONSUMER and the SENDER connect as. The consumer's
# whole job is parsing payloads that the internet influences, so it is the most
# exposed process in the stack. It must not be able to read wuzapi's session keys,
# because those keys ARE the linked WhatsApp device — session material is
# account-takeover material.
#
# So: separate databases, one role each, and PUBLIC's CONNECT revoked on both. The
# archive role ends up unable to reach `wuzapi` even by accident. Do not "simplify"
# this into one database with two schemas unless you have thought about what a
# bug in the parser can then read.
set -euo pipefail

: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${WHATSAPP_APP_PASSWORD:?set WHATSAPP_APP_PASSWORD in your .env}"
: "${WUZAPI_DB_PASSWORD:?set WUZAPI_DB_PASSWORD in your .env}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE whatsapp_app LOGIN PASSWORD '${WHATSAPP_APP_PASSWORD}';
CREATE DATABASE whatsapp OWNER whatsapp_app;

CREATE ROLE wuzapi LOGIN PASSWORD '${WUZAPI_DB_PASSWORD}';
CREATE DATABASE wuzapi OWNER wuzapi;

-- The archive role must not be able to CONNECT to the session store.
REVOKE CONNECT ON DATABASE wuzapi FROM PUBLIC;
REVOKE CONNECT ON DATABASE whatsapp FROM PUBLIC;
SQL

echo "bootstrap: created databases 'whatsapp' (owner whatsapp_app) and 'wuzapi' (owner wuzapi)"
