#!/bin/bash

dir=`dirname $0`
today=`date '+%Y-%m-%d'`
yesterday=`date -d "-1 days" '+%Y-%m-%d'`
archive=`date -d "-1 days" '+%Y%m%d'`
archive_path=/opt/data/influxdb/archives/${archive}
database=$1
sym=$2
mkdir -p ${archive_path}

echo "running 0-2"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T00:00:00Z' and time <'${yesterday}T02:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_02.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T00:00:00Z' and time <'${yesterday}T02:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_02.csv

echo "running 2-4"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T02:00:00Z' and time <'${yesterday}T04:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_04.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T02:00:00Z' and time <'${yesterday}T04:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_04.csv


echo "running 4-6"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T04:00:00Z' and time <'${yesterday}T06:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_06.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T04:00:00Z' and time <'${yesterday}T06:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_06.csv


echo "running 6-8"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T06:00:00Z' and time <'${yesterday}T08:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_08.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T06:00:00Z' and time <'${yesterday}T08:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_08.csv

echo "running 8-10"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T08:00:00Z' and time <'${yesterday}T10:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_10.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T08:00:00Z' and time <'${yesterday}T10:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_10.csv

echo "running 10-12"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T10:00:00Z' and time <'${yesterday}T12:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_12.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T10:00:00Z' and time <'${yesterday}T12:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_12.csv

echo "running 12-14"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T12:00:00Z' and time <'${yesterday}T14:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_14.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T12:00:00Z' and time <'${yesterday}T14:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_14.csv

echo "running 14-16"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T14:00:00Z' and time <'${yesterday}T16:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_16.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T14:00:00Z' and time <'${yesterday}T16:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_16.csv

echo "running 16-18"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T16:00:00Z' and time <'${yesterday}T18:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_18.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T16:00:00Z' and time <'${yesterday}T18:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_18.csv

echo "running 18-20"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T18:00:00Z' and time <'${yesterday}T20:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_20.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T18:00:00Z' and time <'${yesterday}T20:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_20.csv

echo "running 20-22"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T20:00:00Z' and time <'${yesterday}T22:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_22.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T20:00:00Z' and time <'${yesterday}T22:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_22.csv

echo "running 22-24"
${dir}/influx_execute ${database} "select * from OrderBook where time >= '${yesterday}T22:00:00Z' and time <'${today}T00:00:00Z' and symbol='${sym}'"  -format csv > ${archive_path}/${database}_orderbook_${yesterday}_24.csv
${dir}/influx_execute ${database} "select * from Trades where time >= '${yesterday}T22:00:00Z' and time <'${today}T00:00:00Z' and symbol='${sym}'" -format csv > ${archive_path}/${database}_trades_${yesterday}_24.csv
