#!/bin/bash
#判断输入参数是否为两个
if [ $# -ne 2 ]; then
   echo "Input parameter is ERROR!!! please check the usage as below!!!"
   echo "Usage $0  matlab/dl v1/v2"
   exit
fi
#声明版本号
V_VERSION=v46_htl_$1_$2
#V_VERSION=v4_$1_221065_$2
ROOT_DIR=/data/work/bidev/matlab_dl_result
#检测文件/data/work/bidev/matlab_dl_result下的指定的v_version的版本文件夹是否存在
test -e $ROOT_DIR/$V_VERSION
if [ $? -eq 0 ]; then
	rm -r $ROOT_DIR/$V_VERSION
fi 
mkdir $ROOT_DIR/$V_VERSION
echo "mkdir successed"
# 检测hdfs 上的$1_wsf_detail文件夹是否存在
hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail

if [ $? -ne 0 ]; then
	hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail
fi
#检测在hdfs 上的$1_wsf_detail下的dt=$V_VERSION 特定的分区是否存在
hadoop fs -test -e hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail/dt=$V_VERSION

if [ $? -eq 0 ]; then
	hadoop fs -rm -r hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail/dt=$V_VERSION
fi

hadoop fs -mkdir hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail/dt=$V_VERSION

echo "mkdir hdfs successed"
#将loaddata上的数据复制到gateway2上的$ROOT_DIR/$V_VERSION目录下
scp loaddata@192.168.20.209:/data/loaddata/bi_rms/models/data/wsf_result_20160825/$1/$2/$1_wsf_detail*.csv $ROOT_DIR/$V_VERSION/

#scp loaddata@192.168.20.209:/data/loaddata/bi_rms/models/data/wsf_result/$1/$2/$1_wsf_detail_221065*.csv $ROOT_DIR/$V_VERSION/

#将$ROOT_DIR/$V_VERSION目录下的数据上传到hdfs上
hdfs dfs -put $ROOT_DIR/$V_VERSION/* hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail/dt=$V_VERSION/

echo "put wsf data successed"

hive -e "use bwbi_db; alter table $1_pred_valid_wsf_detail DROP IF EXISTS PARTITION (dt='$V_VERSION'); alter table $1_pred_valid_wsf_detail add partition (dt='$V_VERSION') location 'hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail/dt=$V_VERSION';"
echo "output_wsf_path:
	hdfs://ns1/p/bw/bi/rms/middata/history/$1_wsf_detail/dt=$V_VERSION"
