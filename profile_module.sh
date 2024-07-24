#!/bin/bash

FILENAME=""
DATAFOLDER=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -f)
        FILENAME="$2"
        shift 2
        ;;
    -d)
	DATAFOLDER="$2"
	shift 2
	;;
  esac
done

if [[ $FILENAME == "" ]]; then
  echo "You need to specify a filename with the argument -f"
  exit
fi

if [[ $DATAFOLDER == "" ]]; then
  echo "You need to specify a data folder with the argument -d"
  exit
fi


kernprof -l $FILENAME -d $DATAFOLDER
python -m line_profiler $(basename ${FILENAME}).lprof
