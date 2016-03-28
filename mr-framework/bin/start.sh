#!/bin/bash


function prepare()
{
	OLD_WORK_DIR=`pwd`
	NEW_WORK_DIR=`dirname $0`

	source ${NEW_WORK_DIR}/../conf/mrframework.conf
	source ${MRFRAMEWORK_CONF_PATH}/hadoop.conf
	source ${MRFRAMEWORK_BIN_PATH}/shell-utils.sh
	source ${MRFRAMEWORK_BIN_PATH}/task.sh

	mkdir -p ${MRFRAMEWORK_DATA_PATH}

	return 0
}

function check_env()
{
	if [ -z "${JAVA_HOME}" ]
	then
		echo "JAVA_HOME not set" >&2
		return 1
	fi

	if [ ! -d ${HADOOP_HOME_PATH} ]
	then
		echo "HADOOP_HOME_PATH not exist" >&2
		return 1
	fi

	if [ ! -f ${HADOOP_STREAMING_JAR_PATH} ]
	then
		echo "HADOOP_STREAMING_JAR_PATH not exist" >&2
		return 1
	fi

	return 0
}

function usage()
{
	echo "Usage: sh -x start.sh <framework_file> [ -b [<breakpoint_task>] ]" >&2
	return 0
}


function process_framework_conf_file()
{    
	local framework_conf_file=$1
	local breakpoint_tag=$2
	local start_task=$3
	
	if [ -n "${breakpoint_tag}" -a "${breakpoint_tag}" != "-b" ]
	then
		usage
		return 1
	fi

	## 检查framework_conf_file ##
	check_framework_conf_file ${framework_conf_file}
	if [ $? -ne 0 ]
	then
		return 1
	fi

	source ${framework_conf_file} 1>/dev/null

	local curr_schedule_conf_file=${SCHEDULE_CONF_FILE}
	local curr_tasklist_conf_file=${TASKLIST_CONF_FILE}
	
	## check所有任务 ##
	check_task ${curr_schedule_conf_file} ${curr_tasklist_conf_file}
	if [ $? -ne 0 ]
	then
		return 1
	fi

	local user_project_dir=`dirname ${framework_conf_file} | awk '{print $1"/../"}'`
        local breakpoint_file=${user_project_dir}/log/breakpoint/breakpoint

	## check start_task是否ok ##
	local start_task_info=`check_start_task ${curr_schedule_conf_file} ${curr_tasklist_conf_file} \
		${breakpoint_file} ${breakpoint_tag} ${start_task}`
	if [ $? -ne 0 ]
	then
		return 1
	fi

	local real_start_task_type=`echo "${start_task_info}" | awk '{print $1}'`
	local real_start_task=`echo "${start_task_info}" | awk '{$1 = ""; sub("^[ \t]+", "", $0); print $0}'`

	## 调度任务 ##
	local curr_dir=`pwd`
	local work_bin_dir=${user_project_dir}/bin

	cd ${work_bin_dir}
	schedule_task ${curr_schedule_conf_file} ${curr_tasklist_conf_file} ${user_project_dir} \
		${breakpoint_file} "${real_start_task_type}" "${real_start_task}"
	local ret=$?
	if [ ${ret} -ne 0 ]
	then
		echo "schedule ${framework_conf_file} failed" >&2
	else
		echo "schedule ${framework_conf_file} succeed" >&2
	fi
	cd ${curr_dir}

	return ${ret}
}

function check_framework_conf_file()
{
	local framework_conf_file=$1

	source ${framework_conf_file} 1>/dev/null
       
	if [ -z "${SCHEDULE_CONF_FILE}" ]
	then
                
		echo "SCHEDULE_CONF_FILE not exist in ${framework_conf_file}" >&2
		return 1
	fi
        
	if [ ! -f ${SCHEDULE_CONF_FILE} ]
	then
		echo "${SCHEDULE_CONF_FILE} not exit" >&2
		return 1
	fi

	if [ -z "${TASKLIST_CONF_FILE}" ]
	then
		echo "TASKLIST_CONF_FILE not exist in ${framework_conf_file}" >&2
		return 1
	fi
        
	if [ ! -f ${TASKLIST_CONF_FILE} ]
	then
		echo "${TASKLIST_CONF_FILE} not exist" >&2
		return 1
	fi

	return 0
}

function start_job()
{
	cd ${NEW_WORK_DIR}

	local framework_conf_file=$1
	local breakpoint_tag=$2
	local start_task=$3

	if [ ! -f ${framework_conf_file} ]
	then
		echo "$1 not exist" >&2
		return 1
	fi

	process_framework_conf_file ${framework_conf_file} ${breakpoint_tag} ${start_task}

	return $?
}

function finish()
{
    cd ${OLD_WORK_DIR}

    return 0
}


prepare

framework_conf_file=""

if [ $# -ge 1 ]
then
	framework_conf_file=`get_abs_path $1`
	if [ -z "${framework_conf_file}" ]
	then
		echo "$1 not exist" >&2
		exit 1
	fi
else
	usage
	exit 1
fi


check_env
ret=$?
if [ ${ret} -eq 0 ]
then
	start_job ${framework_conf_file} $2 $3
	ret=$?
fi

exit ${ret}

