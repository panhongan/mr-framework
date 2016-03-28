#!/bin/bash

function prepare()
{
	source ../conf/project.conf
	source ../conf/hadoop.conf

	mkdir -p ${LOCAL_DATA_PATH}

	${HADOOP_MKDIR} ${HDFS_WORK_PATH}
	${HADOOP_MKDIR} ${HDFS_INPUT_PATH}
	${HADOOP_MKDIR} ${HDFS_OUTPUT_PATH}

	return 0
}


prepare
