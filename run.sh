#!/bin/bash

MODULE=aggregate2
if [ "$2" != "" ]; then
    MODULE="$2"
fi

erl +SDio 1 +SDPcpu 50 +sbwt none +sbwtdio none -noinput -s "$MODULE" run "$1" -eval "erlang:halt()."
