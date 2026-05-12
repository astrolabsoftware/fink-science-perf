#!/bin/bash
# Copyright 2019-2026 AstroLab Software
# Author: Fabrice Jammes
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

IMAGE="gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-ztf:latest"
FINK_SCIENCE_DIR=""
EXTRA_MOUNTS=""

usage() {
  cat <<EOF
Launch a fink-deps-sentinel-ztf Docker container with workspace mounts.

Usage: $(basename "$0") [OPTIONS]

Options:
  -s <path>   Path to fink-science source directory (mounted at /workspace/fink-science)
  -m <paths>  Colon-separated list of extra paths to mount under /workspace/<basename>
  -h          Show this help

Examples:
  $(basename "$0") \\
    -s /home/user/fink-science \\
    -m /home/user/fink_sn_activelearning:/home/user/other_module
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -s)
      FINK_SCIENCE_DIR="$2"
      shift 2
      ;;
    -m)
      EXTRA_MOUNTS="$2"
      shift 2
      ;;
    -h)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

DOCKER_ARGS=(-t -i --rm)
DOCKER_ARGS+=(-v "${SCRIPT_DIR}:/workspace/fink-science-perf")

# Packages to install in editable mode at container startup (overrides pip-installed versions)
EDITABLE_INSTALLS=()

if [ -n "$FINK_SCIENCE_DIR" ]; then
  ABS_FINK_SCIENCE="$(cd "$FINK_SCIENCE_DIR" && pwd)"
  DOCKER_ARGS+=(-v "${ABS_FINK_SCIENCE}:/workspace/fink-science")
  EDITABLE_INSTALLS+=("/workspace/fink-science")
fi

if [ -n "$EXTRA_MOUNTS" ]; then
  IFS=':' read -ra MOUNT_PATHS <<< "$EXTRA_MOUNTS"
  for path in "${MOUNT_PATHS[@]}"; do
    ABS_PATH="$(cd "$path" && pwd)"
    BASENAME="$(basename "$ABS_PATH")"
    DOCKER_ARGS+=(-v "${ABS_PATH}:/workspace/${BASENAME}")
    EDITABLE_INSTALLS+=("/workspace/${BASENAME}")
  done
fi

# Build a startup command passed to the entrypoint: editable-install each mounted source, then open a shell
# Service startup (Kafka, HBase) is handled by the image entrypoint.
# \$PYTHONPATH is escaped so it expands inside the container, not on the host.
STARTUP="export PYTHONPATH=/workspace/fink-science-perf\${PYTHONPATH:+:\${PYTHONPATH}}"

for pkg in "${EDITABLE_INSTALLS[@]}"; do
  STARTUP="${STARTUP} && pip install -q --no-deps --root-user-action=ignore -e ${pkg}"
done
STARTUP="${STARTUP} && exec bash"

docker run "${DOCKER_ARGS[@]}" "$IMAGE" bash -c "$STARTUP"
