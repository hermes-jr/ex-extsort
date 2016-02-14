#!/bin/bash
# Generate NUM Gigabytes of pseudorandom text split in lines
# usage: ./gendata.sh [gigs]
# Default data size is 1G

LC_CTYPE=C

NUM=1
if [ ! -z ${1} ]; then
	NUM=${1}
fi

cat /dev/urandom | tr -dc "[:alnum:]" | fold -w 64 | head -c $(( ${NUM} * 1024 * 1024 * 1024)) > data.dat
