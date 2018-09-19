#!/bin/bash

db=$1
shift

influx -port 9086 -database ${db} -precision rfc3339 -execute "$@"

