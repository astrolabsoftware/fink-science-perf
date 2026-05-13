#!/bin/bash
# Copyright 2019-2026 AstroLab Software
# Author: Julien Peloton
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

SURVEY=""
NAME=""
DATAFOLDER=""
PROFILE=false

usage() {
  cat <<EOF
Benchmark and profile Fink science modules.

  Find more information on Fink at https://fink-broker.org

Usage: $(basename "$0") [OPTIONS]

Options:
  -s, --survey        Survey name (ztf, rubin). Required.
  -n, --name          Science module name. If omitted, all modules are benchmarked.
  -d, --data          Path to parquet data folder. Required.
  -p, --profile       Enable line-by-line profiling with kernprof (default: throughput only).
  -l, --list-modules  List available science modules and exit.
      --version       Show fink-science version and exit.
  -h, --help          Show this help and exit.

Examples
  List all available modules
    $(basename "$0") --survey ztf --list-modules

  Benchmark the Early SN Ia module (throughput only)
    $(basename "$0") -s ztf -n "Early SN Ia" -d /data/ftransfer_ztf_2026-04-28_434189/

  Profile the Early SN Ia module line by line
    $(basename "$0") -s ztf -n "Early SN Ia" -d /data/ftransfer_ztf_2026-04-28_434189/ --profile
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -s|--survey)
      SURVEY="$2"
      shift 2
      ;;
    -n|--name)
      NAME="$2"
      shift 2
      ;;
    -d|--data)
      DATAFOLDER="$2"
      shift 2
      ;;
    -p|--profile)
      PROFILE=true
      shift 1
      ;;
    -l|--list-modules)
      LIST_MODULES=true
      shift 1
      ;;
    --version)
      python3 -c "import fink_science; print(fink_science.__version__)"
      exit 0
      ;;
    -h|--help)
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

if [[ -z $SURVEY ]]; then
  echo "Error: --survey is required." >&2
  usage >&2
  exit 1
fi

if [[ ${LIST_MODULES} == true ]]; then
  python -c "from ${SURVEY}.science_modules import MODULE_NAMES; print(MODULE_NAMES)"
  exit 0
fi

if [[ -z $DATAFOLDER ]]; then
  echo "Error: --data is required." >&2
  usage >&2
  exit 1
fi

if [[ -z $NAME ]]; then
  echo "No module name specified, all modules will be benchmarked."
  OUTPROF=profiling_all.lprof
else
  OUTPROF="profiling_${NAME// /_}.lprof"
fi

if [[ $PROFILE == true ]]; then
  kernprof -l --outfile "$OUTPROF" "${SURVEY}/prof_science_module.py" \
    -module_name="$NAME" \
    -datafolder="$DATAFOLDER"
  python -m line_profiler -mtz "$OUTPROF"
else
  python "${SURVEY}/prof_science_module.py" \
    -module_name="$NAME" \
    -datafolder="$DATAFOLDER"
fi
