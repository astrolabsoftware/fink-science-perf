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

SINFO="\xF0\x9F\x9B\x88"
SERROR="\xE2\x9D\x8C"
SSTOP="\xF0\x9F\x9B\x91"
SSTEP="\xF0\x9F\x96\xA7"
SDONE="\xE2\x9C\x85"

NAME=""
DATAFOLDER=""
SURVEY=""

# Show help if no arguments is given
if [[ $1 == "" ]]; then
  HELP_ON_SERVICE="-h"
  SURVEY="lsst"
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    -survey)
      SURVEY="$2"
      shift 2
      ;;
    -name)
      NAME="$2"
      shift 2
      ;;
    -d)
      DATAFOLDER="$2"
      shift 2
      ;;
    -h)
      HELP_ON_SERVICE="-h"
      shift 1
      ;;
    --list_modules)
      LIST_MODULES=true
      shift 1
      ;;
    --version)
      python3 -c "import fink_science; print(fink_science.__version__)"
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 1
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

__usage="
Profile science modules in Fink

  Find more information on Fink at https://fink-broker.org

Usage: $(basename $0) [OPTIONS]

Options:
  -survey        Survey name (ztf, rubin). Default is ztf.
  -name          Name of the science module. If not given, profile all science modules.
  -d             Stop a running service.
  -h             Show this help.
  --list_modules If specified, list modules
  --version      If specified, show the version of fink-science

Examples
  List all available modules
  ./profile_module.sh -survey ztf --list_modules

  Profile the early SN Ia module in ZTF
  ./profile_module.sh -survey ztf -name


"

if [[ $SURVEY == "" ]]; then
  echo -e "${SERROR} You need to specify a survey, e.g. fink -s ztf [options]"
  exit 1
fi

if [[ $service == "" ]] && [[ ${HELP_ON_SERVICE} == "-h" ]]; then
  echo -e "$__usage"
  exit 1
fi

if [[ ${LIST_MODULES} == true ]]; then
  python -c "from ${SURVEY}.science_modules import load_${SURVEY}_modules;print(list(load_${SURVEY}_modules().keys()))"
  exit 0
fi

if [[ $NAME == "" ]]; then
  echo "No module name specified, all modules will be profiled."
  OUTPROF=profiling_all.lprof
else
  OUTPROF=profiling_$NAME.lprof
fi

if [[ $DATAFOLDER == "" ]]; then
  echo "You need to specify a data folder with the argument -d"
  exit
fi

if [[ $SURVEY == "" ]]; then
  echo "You need to specify a survey with the argument -survey"
  echo "Available: ztf, rubin"
  exit
fi

kernprof -l --outfile "${OUTPROF// /_}" ${SURVEY}/prof_science_module.py \
	-module_name="$NAME" \
	-datafolder=$DATAFOLDER

python -m line_profiler -mtz "${OUTPROF// /_}"
