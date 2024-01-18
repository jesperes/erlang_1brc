#!/bin/bash

erl +SDio 1 +SDPcpu 50 -noinput -s aggregate2 run "$1" -eval "erlang:halt()."
