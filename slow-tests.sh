#!/usr/bin/env bash

set -e

uv run pytest \
    --log-cli-level=DEBUG \
    --capture=tee-sys \
    -m "slow" \
    --snapshot-diff-mode=detailed \
    -o log_cli=true \
    -o truncation_limit_lines=0 \
    -o truncation_limit_chars=0
