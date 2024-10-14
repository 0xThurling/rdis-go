#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
#
# Learn more: https://codecrafters.io/program-interface
set -e
tmpFile=$(mktemp)
go build -o "$tmpFile" app/*.go
exec "$tmpFile" "$@"
