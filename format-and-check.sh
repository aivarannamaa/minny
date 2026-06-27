#!/usr/bin/env bash

echo "# ruff organize imports #######################################"
uv run ruff check --select I --fix .

echo "# ruff format #################################"
uv run ruff format

echo "# ruff check ##################################"
uv run ruff check

echo "# pyrefly check ###############################"
# perl helps transforming the output to get rid of prefixes, which confuse PyCharm Terminal
uv run pyrefly check --relative-to '' | perl -pe 's/^\s*-->\s+(?=\/)//'

# TODO: ty is still buggy
#ty check