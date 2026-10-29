#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/partner-hub-go-cache}"
go test -race ./... -count=1
go vet ./...
go build ./cmd/...
node --test internal/demo/page_test.js internal/mock/portal/page_behavior_test.js internal/mock/upstream/page_behavior_test.js
node --test web/console_browser_test.js
bash -n scripts/check-console-local.sh
docker compose --env-file .env.example config --quiet
