#!/bin/bash

NAME=""
DATAFOLDER=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -name)
        NAME="$2"
        shift 2
        ;;
    -d)
	DATAFOLDER="$2"
	shift 2
	;;
  esac
done

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

kernprof -l --outfile "${OUTPROF// /_}" ztf/prof_science_module.py \
	-module_name="$NAME" \
	-datafolder=$DATAFOLDER

python -m line_profiler -mtz "${OUTPROF// /_}"
