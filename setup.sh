#!/bin/sh

rm -rf output
hdfs dfs -rm -r /output
hadoop com.sunny.hadooptest.Process6
hdfs dfs -get /output
