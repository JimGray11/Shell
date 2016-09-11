#!/bin/bash
#add by yclai at 2016-09-03 17:32:21 
echo "before,$PATH"
export PATH="/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/hbase/bin:/usr/local/hive/bin:/usr/local/hive/hcatalog/bin:/usr/local/mysql/bin:/usr/local/oozie/bin:/usr/local/sqoop/bin:/usr/local/scala/bin:/usr/local/spark-1.5.1/bin:/usr/local/bin:/usr/local/jdk/bin:/usr/local/maven/bin:/usr/local/ant/bin:/usr/local/protobuf/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/home/bidev/bin"
#JAVA_HOME=/usr/local/jdk
#HADOOP_HOME=/usr/local/hadoop
#HIVE_HOME=/usr/local/hive
#export PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin:/sbin:$JAVA_HOME/bin
echo "after, $PATH"
if [ $# -eq 3 ]; then
 V_DT=$(date -d "-1 minutes" +%Y%m%d)
elif [ $# -eq 4 ]; then
 V_DT=$4
 echo "the yesterday is $V_DT"
else 
  echo "Input parameter is ERROR!!! please check the usage as below!!!"
  echo "Usage $0 version pu_type curve_type dataDAY"
  echo "Usage $0 daily_online DAY BK 20160903"
  exit 1
fi

echo "$0 start $V_DT"


ROOT_DIR=/data/work/bidev/sqlquery

V_VERSION=$1
V_PU_TYPE=$2
V_CURVE_TYPE=$3

grep -r -l "V_VERSION" $ROOT_DIR/conf/application.conf_DEMO_online_HOURLY |xargs sed -e "s/V_VERSION/$V_VERSION/g" -e "s/V_PU_TYPE/$V_PU_TYPE/g" -e "s/V_CURVE_TYPE/$V_CURVE_TYPE/g" -e "s/V_DT/$V_DT/g" > $ROOT_DIR/conf/application.conf_${V_VERSION}_HOURLY

hive_table_name=("online_pred_valid_sf_detail") 
hive_table_dir=("online_sf_detail") 


for ((i=0;i<${#hive_table_dir[*]};i++)) 
do

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/hourly

if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/hourly
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/hourly/dt=${V_VERSION}_${V_DT}
if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/hourly/dt=${V_VERSION}_${V_DT}
fi

done

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/pred_fit/dt=$V_VERSION
if [ $? -ne 0 ]; then
   echo "the hdfs file hdfs://ns1/p/bw/bi/rms/middata/history/pred_fit/dt=$V_VERSION is not exist!!!"
   echo "$0 will be exit with errorcode 2"
   exit 2
fi

hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/fix_pred_fit_sf_sum/dt=$V_VERSION
if [ $? -ne 0 ]; then
   echo "the hdfs file hdfs://ns1/p/bw/bi/rms/middata/history/fix_pred_fit_sf_sum/dt=$V_VERSION is not exist!!!"
   echo "$0 will be exit with errorcode 2"
   exit 2
fi

/usr/local/spark-1.5.1/bin/spark-submit --jars $(echo ${ROOT_DIR}/assemblies/*.jar | tr ' ' ',') --class cn.jw.rms.data.framework.core.Entrance --master yarn-client --executor-memory 4G --driver-memory 8G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/framework-core-assembly-1.3.jar ${ROOT_DIR}/conf/application.conf_${V_VERSION}_HOURLY


hive -e "use bwbi_db; alter table online_pred_src_actual DROP IF EXISTS PARTITION (dt='$V_VERSION'); alter table online_pred_src_actual  add partition (dt='$V_VERSION') location '/p/bw/bi/rms/middata/history/online_actual/dt=$V_VERSION';"

hive -e "use bwbi_db; alter table online_pred_valid_all_detail DROP IF EXISTS PARTITION (dt='$V_VERSION'); alter table online_pred_valid_all_detail  add partition (dt='$V_VERSION') location '/p/bw/bi/rms/middata/history/online_all_detail/dt=$V_VERSION';"


if [ ! -d $ROOT_DIR/data/$V_VERSION ]; then
  mkdir $ROOT_DIR/data/$V_VERSION
fi

if [ ! -d $ROOT_DIR/data/$V_VERSION/$V_DT ]; then
  mkdir $ROOT_DIR/data/$V_VERSION/$V_DT
fi

for ((i=0;i<${#hive_table_dir[*]};i++))
do

if [ -d $ROOT_DIR/data/$V_VERSION/$V_DT/${hive_table_dir[$i]} ]; then
  rm -f $ROOT_DIR/data/$V_VERSION/$V_DT/${hive_table_dir[$i]}/${V_VERSION}_${V_DT}_part-00000
fi
  mkdir $ROOT_DIR/data/$V_VERSION/$V_DT/${hive_table_dir[$i]}

hadoop fs -cat hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/hourly/dt=${V_VERSION}_${V_DT}/part* > $ROOT_DIR/data/$V_VERSION/$V_DT/${hive_table_dir[$i]}/${V_VERSION}_${V_DT}_part-00000

hadoop fs -rm -r hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/dt=${V_VERSION}/${V_VERSION}_${V_DT}_part-00000
hadoop fs -put $ROOT_DIR/data/$V_VERSION/$V_DT/${hive_table_dir[$i]}/${V_VERSION}_${V_DT}_part-00000  hdfs://ns1/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/dt=${V_VERSION}/


done
echo "$0 execute $V_DT successfully"
#hive -hivevar qtime="$V_VERSION" -f $ROOT_DIR/hql/002_online_hour_pred_valid_all_detail.hql
exit 0
