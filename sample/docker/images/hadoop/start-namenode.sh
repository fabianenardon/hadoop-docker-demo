#!/bin/bash

# We should format namenode only once. So, when we format it
# we save a marker file.

if [ ! -f /data/.already_formatted ]; then
  echo FORMATTING NAMENODE

  mkdir -p /data/dfs/data
  mkdir -p /data/dfs/name
  mkdir -p /data/dfs/namesecondary

  hdfs namenode -format
  touch /data/.already_formatted
fi

hdfs namenode