#!/usr/bin/env bash

set -euo pipefail
exec java ${JAVA_OPTS:-} -jar /app/chaincode.jar