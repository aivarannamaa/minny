#!/usr/bin/env bash

# use with --snapshot-update to update snapshots

set -euo pipefail

uv run pytest \
    --log-cli-level=WARNING \
    --capture=tee-sys \
    -m "not slow" \
    --snapshot-diff-mode=detailed \
    -o log_cli=true \
    -o truncation_limit_lines=0 \
    -o truncation_limit_chars=0  \
    "$@"
