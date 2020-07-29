#!/bin/bash


##! @VERSION:   1.1
##! @AUTHOR:    phadyc@126.com
##! @FILEIN:    
##! @FILEOUT:
##! @DEP:
##! @PREV:
##! @NEXT:


function lock_index_timeout()
{
	local index_path=$1
	local lock_timeout=180
	if [ $# -eq 2 ]
	then
		lock_timeout=$2
	fi

	local lock_timeout_bak=${lock_timeout}
	
	mkdir -p $index_path

	while true
	do
		mkdir $index_path/lock &>/dev/null
		if [ $? -eq 0 ]
		then
			#echo `date +%s` >$index_path/lock/timestamp
			break
		else
			lock_timeout=`expr ${lock_timeout} - 10`
			if [ ${lock_timeout} -lt 0 ]
			then
				send_mail_msg -t 2 -s "wait for lock timeout ${lock_timeout_bak} secs" -p ""
				return -1;
			else
				sleep 10
			fi
		fi
	done

	return 0
}

function lock_index()
{
	if [ $# -ne 1 ]
	then
		echo "lock_index(): invalid parameter"
		return 1
	fi

	local index_path=$1
	mkdir -p $index_path

	while true
	do
		mkdir $index_path/lock &>/dev/null
		if [ $? -eq 0 ]
		then
			break
		else
			sleep 3
		fi
	done

	return 0
}

function unlock_index()
{
	if [ $# -ne 1 ]
	then
		echo "unlock_index(): invalid parameter"
		return 1
	fi

	local index_path=$1
	rm -rf $index_path/lock

	return 0
}

function trim_string()
{
	echo "$1" | awk '{
		sub("^[ \t]+", "", $0)
		sub("[ \t]+$", "", $0)
		print $0
	}'

	return 0
}


function del_overtime_file()
{
	if [ $# -ne 2 ]
	then
		echo "del_overtime_file(): invalid parameter"
		return 1
	fi

	local dst_dir=$1
	local keep_time_hour=$2 

	local del_time=`date +%s -d"-${keep_time_hour} hour"`

	for file  in `ls ${dst_dir}`
	do
		local change_time=`stat -c %Y ${dst_dir}/${file}`
		if [ ${change_time} -lt ${del_time} ]
		then
			rm -f ${dst_dir}/${file}
		fi
	done

	return 0
}

function get_abs_path()
{
	local curr_path=$1

	if [ -n "${curr_path}" ]
	then
		if [ -f ${curr_path} ]
		then
			local curr_work_dir=`pwd`

			local file_dir=`dirname ${curr_path}`
			local file_name=`basename ${curr_path}`
			file_dir=`cd ${file_dir} && pwd`
			echo ${file_dir}"/"${file_name}
			cd ${curr_work_dir}
		elif [ -d ${curr_path} ]
		then
			local curr_work_dir=`pwd`
			echo `cd ${curr_path} && pwd`
			cd ${curr_work_dir}
		fi
	else
		echo ""
	fi

	return 0	
}

