#!/bin/bash

hdfs dfs -mkdir -p /mr-history/tmp 
hdfs dfs -chmod -R 1777 /mr-history/tmp 

hdfs dfs -mkdir -p /mr-history/done 
hdfs dfs -chmod -R 1777 /mr-history/done

hdfs dfs -mkdir -p /app-logs
hdfs dfs -chmod -R 1777 /app-logs

hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging
hdfs dfs -chmod -R 1777 /tmp/hadoop-yarn/staging

mr-jobhistory-daemon.sh --config $HADOOP_HOME/etc/hadoop start historyserver

yarn resourcemanager