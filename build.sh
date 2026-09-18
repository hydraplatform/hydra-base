#!/usr/bin/env bash
set -euo pipefail   # fail on error, missing vars, or pipe fails

echo "▶ Cleaning previous artefacts (if any)…"
for dir in dist build; do
    if [[ -d "$dir" ]]; then
        echo "  • Removing $dir"
        rm -rf "$dir"
    fi
done

shopt -s nullglob          # *.egg-info expands to empty list if none
for egg in *.egg-info; do
    echo "  • Removing $egg"
    rm -rf "$egg"
done
shopt -u nullglob          # restore default

echo "▶ Building package…"
python -m build

echo "▶ Uploading to PyPI…"
twine upload dist/*
