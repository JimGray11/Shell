#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21 

if [ $# -ne 1 ]; then
   echo "Input parameter is ERROR!!! please check the usage as below!!!"
   echo "Usage $0 version"
   exit
fi

ROOT_DIR=/data/work/bidev/sqlquery

V_VERSION=$1

sh -x /usr/local/spark-1.5.1/bin/spark-submit --jars $(echo ${ROOT_DIR}/assemblies/*.jar | tr ' ' ',') --class cn.jw.rms.data.framework.core.Entrance --master yarn-client --executor-memory 4G --driver-memory 8G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/framework-core-assembly-1.3.jar ${ROOT_DIR}/conf/application.conf_${V_VERSION}

hive_table_name=("online_pred_src_actual"  "online_pred_valid_lf_detail"  "online_pred_valid_sf_detail"  "online_pred_valid_wsf_detail" "online_pred_valid_all_detail") 
hive_table_dir=("online_actual"  "online_lf_detail"  "online_sf_detail"  "online_wsf_detail" "online_all_detail")

for ((i=0;i<${#hive_table_name[*]};i++)) 
do

hive -e "use bwbi_db; alter table ${hive_table_name[$i]} DROP IF EXISTS PARTITION (dt='$V_VERSION'); alter table ${hive_table_name[$i]}  add partition (dt='$V_VERSION') location '/p/bw/bi/rms/middata/history/${hive_table_dir[$i]}/dt=$V_VERSION';"
done


#echo "
	/usr/bin/impala-shell -q 'invalidate metadata;'

#   " | ssh hadoop@s1 
#echo "Start hdfs2mysql"
#sh -x ${ROOT_DIR}/hdfs2mysql.sh
