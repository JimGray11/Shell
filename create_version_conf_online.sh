#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21 

if [ $# -ne 3 ]; then
        echo "Input parameter number is ERROR!!! please check the usage as below!!!"
	echo "Usage $0 version pu_typ curve_typ"
	echo "$0 v13 DAY BK"
	exit 0
fi
ROOT_DIR=/data/work/bidev/sqlquery

V_VERSION=$1
V_PU_TYP=$2
V_CURVE_TYP=$3
#V_SEG_BK_DAILY_SUM=$4

mid_dir=("online_actual" "online_lf_detail" "online_sf_detail" "online_wsf_detail" "online_all_detail") 

for ((i=0;i<${#mid_dir[*]};i++)) 
do

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/${mid_dir[$i]}

if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/${mid_dir[$i]}
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/${mid_dir[$i]}/dt=$V_VERSION
if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/${mid_dir[$i]}/dt=$V_VERSION
	#hadoop fs -put /data/work/bidev/online_dl_result/$V_VERSION/${mid_dir[$i]}_*  hdfs://ns1/p/bw/bi/rms/middata/history/${mid_dir[$i]}/dt=$V_VERSION/
        #echo "/data/work/bidev/online_dl_result/$V_VERSION/${mid_dir[$i]}_*"
fi
done

grep -r -l "V_VERSION" $ROOT_DIR/conf/application.conf_DEMO_online_v2 | xargs sed -e "s/V_VERSION/$V_VERSION/g" -e "s/V_PU_TYP/$V_PU_TYP/g" -e "s/V_CURVE_TYP/$V_CURVE_TYP/g" > $ROOT_DIR/conf/application.conf_$V_VERSION

