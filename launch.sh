#!/bin/bash

# Launch minny with uv run and pass all script arguments to it
exec uv run python -m minny "$@"
