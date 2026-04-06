#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if command -v buf >/dev/null 2>&1; then
    BUF_BIN="$(command -v buf)"
elif [ -x "${HOME}/go/bin/buf" ]; then
    BUF_BIN="${HOME}/go/bin/buf"
else
    echo "buf CLI not found. Install with:" >&2
    echo "  go install github.com/bufbuild/buf/cmd/buf@v1.46.0" >&2
    exit 1
fi

cd "${ROOT_DIR}"

"${BUF_BIN}" generate

# Ensure Python package dirs are importable in both service trees.
for base in \
    "${ROOT_DIR}/../../../../diri-cyrex/app/integrations/streaming/gen" \
    "${ROOT_DIR}/../../../../diri-helox/integrations/streaming/gen"
do
    mkdir -p "${base}/synapse/v1" "${base}/proto/synapse/v1"
    touch "${base}/proto/__init__.py"
    touch "${base}/proto/synapse/__init__.py"
    touch "${base}/proto/synapse/v1/__init__.py"
    touch "${base}/synapse/__init__.py"
    touch "${base}/synapse/v1/__init__.py"
done

echo "Proto generation complete."
