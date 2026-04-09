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

ensure_python_pkg_dirs() {
    local base="$1"
    mkdir -p "${base}/synapse/v1" "${base}/proto/synapse/v1"
    touch "${base}/proto/__init__.py"
    touch "${base}/proto/synapse/__init__.py"
    touch "${base}/proto/synapse/v1/__init__.py"
    touch "${base}/synapse/__init__.py"
    touch "${base}/synapse/v1/__init__.py"
}

"${BUF_BIN}" generate --template "${ROOT_DIR}/buf.gen.yaml"

CYREX_PY_GEN_OUT="${CYREX_PY_GEN_OUT:-}"
HELOX_PY_GEN_OUT="${HELOX_PY_GEN_OUT:-}"

if [ -n "${CYREX_PY_GEN_OUT}" ] || [ -n "${HELOX_PY_GEN_OUT}" ]; then
    PY_TEMPLATE="$(mktemp)"
    trap 'rm -f "${PY_TEMPLATE}"' EXIT

    {
        echo "version: v1"
        echo
        echo "plugins:"
        if [ -n "${CYREX_PY_GEN_OUT}" ]; then
            echo "  - plugin: buf.build/protocolbuffers/python"
            echo "    out: ${CYREX_PY_GEN_OUT}"
            echo "  - plugin: buf.build/grpc/python"
            echo "    out: ${CYREX_PY_GEN_OUT}"
        fi
        if [ -n "${HELOX_PY_GEN_OUT}" ]; then
            echo "  - plugin: buf.build/protocolbuffers/python"
            echo "    out: ${HELOX_PY_GEN_OUT}"
            echo "  - plugin: buf.build/grpc/python"
            echo "    out: ${HELOX_PY_GEN_OUT}"
        fi
    } > "${PY_TEMPLATE}"

    "${BUF_BIN}" generate --template "${PY_TEMPLATE}"

    if [ -n "${CYREX_PY_GEN_OUT}" ]; then
        ensure_python_pkg_dirs "${CYREX_PY_GEN_OUT}"
    fi
    if [ -n "${HELOX_PY_GEN_OUT}" ]; then
        ensure_python_pkg_dirs "${HELOX_PY_GEN_OUT}"
    fi
fi

echo "Proto generation complete (Go stubs updated)."
