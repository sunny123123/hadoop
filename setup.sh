#!/bin/sh

rm -rf output
hdfs dfs -rm -r /output
hadoop com.sunny.hadooptest.Process7
hdfs dfs -rm -r /mars_tianchi_everysongs_statics.csv
hdfs dfs -cp /output/part-r-00000 /mars_tianchi_everysongs_statics.csv
hdfs dfs -get /output  ./p2_song_statistics_type1
hdfs dfs -rm -r /output
hadoop com.sunny.hadooptest.Process6
hdfs dfs -get /output ./p2_singer_statistics_type1
