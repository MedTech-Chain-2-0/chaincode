#!/usr/bin/env bash
# Simple helper: copy the FHE bits needed for compiling `bfv_calc` into ../fhe-src.

set -e

# Determine script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAINCODE_DIR="${SCRIPT_DIR}/.."    # one level up from scripts/
ROOT_DIR="${SCRIPT_DIR}/../.."       # repo root (contains fhe/)
FHE_DIR="${ROOT_DIR}/fhe"
DEST="${CHAINCODE_DIR}/fhe-src"

if [[ ! -d "${FHE_DIR}" ]]; then
  echo "[copy-fhe] ERROR: expected '${FHE_DIR}' but it does not exist." >&2
  exit 1
fi

echo "[copy-fhe] Preparing ${DEST}…"
rm -rf "${DEST}"
mkdir -p "${DEST}"

# Minimal copy
cp "${FHE_DIR}/CMakeLists.txt" "${DEST}/"
cp -r "${FHE_DIR}/src" "${DEST}/"

echo "[copy-fhe] Done. FHE source is at ${DEST}"