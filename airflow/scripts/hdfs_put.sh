#!/bin/bash

hadoop fs -mkdir -p datadir
hdfs dfs -put /tmp/data/$1/$2 datadir
