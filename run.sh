#!/bin/bash

MODULE=aggregate2
if [ "$2" != "" ]; then
    MODULE="$2"
fi

# +SDio 1 +SDPcpu 50

erl +sbwt none +sbwtdio none +JPperf true -noinput -s "$MODULE" run "$1" -eval "erlang:halt()."
