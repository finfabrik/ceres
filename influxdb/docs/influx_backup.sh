#!/bin/bash

today=`date '+%Y-%m-%d'`
yesterday=`date -d "-1 days" '+%Y-%m-%d'`
db=cryptocompare
rp=autogen
datadir=/opt/apps/iris/db/data
waldir=/opt/apps/iris/influx/wal
outdir=/opt/data/influxdb

echo Running influx_inspect export -database $db -retention $rp -datadir $datadir -waldir $waldir -compress -start ${yesterday}T00:00:00Z -end ${today}T00:00:00Z -out $outdir/export.${db}-${yesterday}.lineproto.gz

influx_inspect export -database $db -retention $rp -datadir $datadir -waldir $waldir -compress -start ${yesterday}T00:00:00Z -end ${today}T00:00:00Z -out $outdir/export.${db}-${yesterday}.lineproto.gz

echo Done
