#!/bin/bash
LC_CTYPE=C
# Generate 4Gigabytes of pseudorandom text split in lines
cat /dev/urandom | tr -dc "[:alnum:]" | fold -w 64 | head -c $((4*1024*1024*1024)) > data.dat
