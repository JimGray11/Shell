#!/bin/bash
#add by yclai at 2016-09-03 17:32:21 

if [ $# -ne 3 ]; then
   echo "Input parameter is ERROR!!! please check the usage as below!!!"
   echo "Usage $0 version pu_type curve_type"
   echo "Usage $0 daily_online DAY BK"
   exit 1
fi

ROOT_DIR=/data/work/bidev/sqlquery

V_VERSION=$1
V_PU_TYPE=$2
V_CURVE_TYPE=$3

grep -r -l "V_VERSION" $ROOT_DIR/conf/application.conf_DEMO_online_v2 |xargs sed -e "s/V_VERSION/$V_VERSION/g" -e "s/V_PU_TYPE/$V_PU_TYPE/g" -e "s/V_CURVE_TYPE/$V_CURVE_TYPE/g" > $ROOT_DIR/conf/application.conf_${V_VERSION}

hive_table_name=("online_pred_src_actual"  "online_pred_valid_lf_detail"  "online_pred_valid_sf_detail"  "online_pred_valid_wsf_detail") 
hive_table_dir=("online_actual"  "online_lf_detail"  "online_sf_detail"  "online_wsf_detail")


hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/online_pred_daily

if [ $? -ne 0 ]; then
        hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/online_pred_daily
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/online_pred_daily/dt=$V_VERSION

if [ $? -ne 0 ]; then
        hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/online_pred_daily/dt=$V_VERSION
fi

for ((i=0;i<${#hive_table_dir[*]};i++)) 
do

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}

if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/dt=$V_VERSION
if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/dt=$V_VERSION
fi

done

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/pred_fit/dt=$V_VERSION
if [ $? -ne 0 ]; then
   echo "the hdfs file hdfs://ns1/p/bw/bi/rms/middata/history/pred_fit/dt=$V_VERSION is not exist!!!"
   echo "$0 will be exit with errorcode 2"
   exit 2
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/pred_fit_lf_sum/dt=$V_VERSION

if [ $? -ne 0 ]; then
   echo "the hdfs file hdfs://ns1/p/bw/bi/rms/middata/history/pred_fit_lf_sum/dt=$V_VERSION is not exist!!!"
   echo "$0 will be exit with errorcode 2"
   exit 2
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/split_pred_wsf_sum/dt=$V_VERSION
if [ $? -ne 0 ]; then
   echo "the hdfs file hdfs://ns1/p/bw/bi/rms/middata/history/split_pred_wsf_sum/dt=$V_VERSION is not exist!!!"
   echo "$0 will be exit with errorcode 2"
   exit 2
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/fix_pred_fit_sf_sum/dt=$V_VERSION
if [ $? -ne 0 ]; then
   echo "the hdfs file hdfs://ns1/p/bw/bi/rms/middata/history/fix_pred_fit_sf_sum/dt=$V_VERSION is not exist!!!"
   echo "$0 will be exit with errorcode 2"
   exit 2
fi

sh -x /usr/local/spark-1.5.1/bin/spark-submit --jars $(echo ${ROOT_DIR}/assemblies/*.jar | tr ' ' ',') --class cn.jw.rms.data.framework.core.Entrance --master yarn-client --executor-memory 4G --driver-memory 8G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/framework-core-assembly-1.3.jar ${ROOT_DIR}/conf/application.conf_${V_VERSION}


for ((i=0;i<${#hive_table_name[*]};i++))
do

hive -e "use bwbi_db; alter table ${hive_table_name[$i]} DROP IF EXISTS PARTITION (dt='$V_VERSION'); alter table ${hive_table_name[$i]}  add partition (dt='$V_VERSION') location 'hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/dt=$V_VERSION';"

done

hive -hivevar qtime="$V_VERSION" -f $ROOT_DIR/hql/001_online_pred_valid_all_detail.hql

exit 0
