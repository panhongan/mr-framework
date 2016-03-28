#!/bin/bash

function prepare()
{
	source ${MRFRAMEWORK_CONF_PATH}/mrframework.conf
 
	mkdir -p ${MRFRAMEWORK_LOG_PATH}
	mkdir -p ${MRFRAMEWORK_SCHEDULE_PATH}

	return 0
}

function check_task()
{
	local schedule_conf_file=$1
	local tasklist_conf_file=$2

	local has_task=0

	source ${tasklist_conf_file} 1>/dev/null

	while read -r task_type task_list
	do
		if [ -z "${task_type}" -o "${task_type:0:1}" == "#" ]
		then
			continue
		fi

		has_task=1

		task_type=`echo "${task_type}" | awk '{
			sub("^[\\\\[]", "", $1)
			sub("[\\\\]]$", "", $1)
			print $1
		}'`

		local is_valid=`is_valid_task_type ${task_type}`
		if [ ${is_valid} -eq 0 ]
		then
			echo "Invalid task type : ${task_type}" >&2
			return 1
		fi

  		for task_name in `echo "${task_list}"`
		do
			local checked_task_type=`check_task_type ${task_name} ${task_type}`
			if [ $? -eq 0 -a "${checked_task_type}" == "${task_type}" ]
			then
				continue
   			else
				echo "${task_name} not exist in ${tasklist_conf_file}" >&2
				return 1
   			fi
		done
	done <${schedule_conf_file}

	if [ ${has_task} -eq 0 ]
	then
		echo "No task to be scheduled in ${schedule_conf_file}" >&2
		return 1
	fi

	return 0
}

function check_start_task()
{
	local schedule_conf_file=$1
	local tasklist_conf_file=$2
	local breakpoint_file=$3
	local breakpoint_tag=$4
	local start_task=$5

	local real_start_task_info=""

 	source ${tasklist_conf_file} 1>/dev/null

	if [ -n "${breakpoint_tag}" ]
	then
		if [ -n "${start_task}" ]
		then
			local start_task_info=`get_start_task_info ${schedule_conf_file} ${start_task}`
			local start_task_type=`echo "${start_task_info}" | awk '{print $1}'`
			if [ -z "${start_task_type}" ]
			then
				echo "start_task not exist in ${schedule_conf_file} : ${start_task}" >&2
				return 1
			fi

			real_start_task_info="${start_task_info}"
		else
			local breakpoint_task_info=`get_breakpoint_task_info ${breakpoint_file}`
			local breakpoint_task_type=`echo "${breakpoint_task_info}" | awk '{print $1}'`
			local breakpoint_task=`echo "${breakpoint_task_info}" | awk '{$1 = ""; print $0}'`

			if [ -n "${breakpoint_task_type}" -a -n "${breakpoint_task}" ]
			then
				local is_ok=1

				for task_name in `echo "${breakpoint_task}"`
				do
					## 检查断点任务属性是否变更，若是变更，则忽略该断点任务 ##
					local checked_task_type=`check_task_type ${task_name} ${breakpoint_task_type}`
					if [ $? -eq 0 -a "${checked_task_type}" == "${breakpoint_task_type}" ]
					then
						:
					else
						echo "breakpoint task has been changed in ${tasklist_conf_file}, ignore breakpoint task : ${breakpoint_task_info}" >&2
						is_ok=0
						break
					fi
				done

				if [ ${is_ok} -eq 1 ]
				then
					real_start_task_info="${breakpoint_task_info}"
				fi
			fi
		fi
	fi

	echo "${real_start_task_info}"

	return 0
}

function get_start_task_info()
{
	local schedule_conf_file=$1
	local start_task=$2

	cat ${schedule_conf_file} | awk '{
  		if ($1 ~ "^#")
  		{
   			next
  		}

  		sub("^[\\[]", "", $1)
  		sub("[\\]]$", "", $1)
 
  		for (i = 2; i <= NF; ++i)
  		{
   			if ($i == "'${start_task}'")
   			{
					if ($1 == "'${TASK_TYPE_LOCAL}'" || $1 == "'${TASK_TYPE_MAPREDUCE}'")
    				{
     					print $1, $i
    				}
    				else if ($1 == "'${TASK_TYPE_PARALLEL_MAPREDUCE}'")
    				{
     					task_type = $1
     					$1 = ""
     					print task_type, $0 
    				}

    				exit 0
   			}
  		}
	}'
        
 	return 0
}

function get_breakpoint_task_info()
{
	local breakpoint_file=$1

	if [ -f ${breakpoint_file} ]
	then
		while read -r task_type breakpoint_task
		do
			if [ -n "${task_type}" -a -n "${breakpoint_task}" ]
			then
				echo "${task_type} ${breakpoint_task}"
				return 0
			fi
		done <${breakpoint_file}
	else
		echo ""
	fi

	return 0
}

function schedule_task()
{
	local schedule_conf_file=$1
	local tasklist_conf_file=$2
	local user_project_dir=$3
	local breakpoint_file=$4
	local start_task_type=$5
	local start_task=$6

	local is_start_task_scheduled=0

	if [ -z "${start_task_type}" ]
	then
		is_start_task_scheduled=1
	fi

	source ${tasklist_conf_file} 1>/dev/null

	mkdir -p ${user_project_dir}/log
	mkdir -p `dirname "${breakpoint_file}"`

	while read -r task_type task_list
	do               
		if [ -z "${task_type}" -o "${task_type:0:1}" == "#" ]
		then
			continue
		fi      
  		
		task_type=`echo "${task_type}" | awk '{
                        sub("^[\\\\[]", "", $1)
                        sub("[\\\\]]$", "", $1)
                        print $1
                }'`
                
  		if [ ${is_start_task_scheduled} -eq 1 ]
		then
			schedule_task_exec "${task_type}" "${task_list}" ${user_project_dir} ${breakpoint_file}
			if [ $? -ne 0 ]
			then
				return 1
			fi
		else
			if [ "${task_type}" == "${start_task_type}" ]
			then
    			local is_hit_start_task=0
    			local tmp_task_list=""

    			if [ "${task_type}" != "${TASK_TYPE_PARALLEL_MAPREDUCE}" ]
    			then 
     				for task_name in `echo "${task_list}"`
     				do
      					if [ ${is_hit_start_task} -eq 1 ]
      					then
       						tmp_task_list="${tmp_task_list} ${task_name}"
      					else
       						if [ "${task_name}" == "${start_task}" ]
       						then
        						tmp_task_list="${tmp_task_list} ${task_name}"
        						is_hit_start_task=1
       						fi
      					fi
     				done
    			else
     				local revised_task_list=`format_by_space "${task_list}"`
     				local revised_start_task_list=`format_by_space "${start_task}"`

     				if [ "${revised_task_list}" == "${revised_start_task_list}" ]
     				then
      					tmp_task_list="${revised_start_task_list}"
      					is_hit_start_task=1
     				fi
    			fi

    			if [ ${is_hit_start_task} -eq 1 ]
    			then 
     				schedule_task_exec "${task_type}" "${tmp_task_list}" ${user_project_dir} ${breakpoint_file}
     				if [ $? -ne 0 ]
     				then
      					return 1
     				fi

     				is_start_task_scheduled=1
    			fi
   			fi
  		fi
 	done <${schedule_conf_file}

	## 清空断点文件 ##
 	:>${breakpoint_file}

 	return 0
}

function schedule_task_exec()
{
 	local task_type=$1
	local task_list=$2
 	local user_project_dir=$3
 	local breakpoint_file=$4
      
	if [ "${task_type}" == "${TASK_TYPE_LOCAL}" ]
	then
  		for task_name in `echo "${task_list}"`
  		do
   			local log_file=${user_project_dir}/log/schedule_local_${task_name}.err

			schedule_local_task ${task_name} ${log_file}
			if [ $? -eq 0 ]
			then
    			echo "schedule ${task_type} task succeed : ${task_type} ${task_name}" >&2
   			else
				echo "schedule ${task_type} task failed : ${task_type} ${task_name}" >&2
				save_breakpoint "${task_type}" "${task_name}" "${breakpoint_file}"
				return 1
			fi
		done
 	elif [ "${task_type}" == "${TASK_TYPE_MAPREDUCE}" ]
 	then
  		for task_name in `echo "${task_list}"`
  		do
   			local log_file=${user_project_dir}/log/schedule_mapreduce_${task_name}.err

			schedule_mr_task ${task_name} ${log_file}
			if [ $? -eq 0 ]
			then
				echo "schedule ${task_type} task succeed : ${task_type} ${task_name}" >&2
   			else
				echo "schedule ${task_type} task failed : ${task_type} ${task_name}" >&2
				save_breakpoint "${task_type}" "${task_name}" "${breakpoint_file}"
				return 1
			fi
  		done       
 	elif [ "${task_type}" == "${TASK_TYPE_PARALLEL_MAPREDUCE}" ]       
 	then      
  		schedule_parallel_mr_task "${task_list}" ${user_project_dir}    
  		if [ $? -eq 0 ]
  		then
   			echo "schedule ${task_type} task succeed : ${task_type} ${task_list}" >&2
  		else
   			echo "schedule ${task_type} task failed : ${task_type} ${task_list}" >&2
			save_breakpoint "${task_type}" "${task_list}" "${breakpoint_file}"
			return 1
  		fi
	elif [ "${task_type}" == "${TASK_TYPE_PARALLEL_LOCAL}" ]
	then
		schedule_parallel_local_task "${task_list}" ${user_project_dir}
		if [ $? -eq 0 ]
		then
			echo "schedule ${task_type} task succeed : ${task_type} ${task_list}" >&2
		else
			echo "schedule ${task_type} task failed : ${task_type} ${task_list}" >&2
			save_breakpoint "${task_type}" "${task_list}" "${breakpoint_file}"
			return 1
		fi
 	else
  		echo "Invalid task type : ${task_type} ${task_list} in ${schedule_conf_file}" >&2
 	fi

	return 0
}

function schedule_local_task()
{
	local task_name=$1
	local log_file=$2

	local ret=1

	for((i = 0; i < ${LOCAL_TASK_NUM}; ++i))
	do
		if [ "${task_name}" == "${LOCAL_TASK_NAME[$i]}" ]
		then
			if [ -n "${LOCAL_TASK_CMD[$i]}" ]
			then
				local cmd="${LOCAL_TASK_CMD[$i]} 2>${log_file}"
				eval "${cmd}"
				ret=$?
			else
				echo "LOCAL_TASK_CMD[$i] is empty" >&2
				ret=1
			fi

			break
		fi
	done

	if [ $ret -eq 0 ]
	then
		echo "${JOB_SUCCEED_TAG}" >&2
	else
		echo "${JOB_FAILED_TAG}" >&2
	fi

	return 0
}

function schedule_mr_task()
{
	local task_name=$1
	local log_file=$2

  	rm -f ${log_file}

	for((i = 0; i < ${MR_TASK_NUM}; ++i))
	do
		if [ "${task_name}" == "${MR_TASK_NAME[$i]}" ]
		then
			is_mr_task_ok $i
			if [ $? -eq 0 ]
			then
				return 1
			fi

			${HADOOP_RMR} ${MR_TASK_OUTPUT[$i]}

			local cmd=`construct_mr_task_cmd $i`
			cmd="${cmd} 2>${log_file}"
			eval "${cmd}"
			local ret=$?
			if [ ${ret} -eq 0 ]
			then
				is_task_finished ${log_file}
				if [ $? -eq ${JOB_FAILED} ]
				then
					ret=1
				fi
			fi

			return ${ret}
		fi
	done

	return 0
}

function schedule_parallel_mr_task()
{
	local task_list=$1
	local user_project_dir=$2

	local curr_time=`date +%Y%m%d_%H%M%S_%s%N`
	local schedule_parallel_task_file=${MRFRAMEWORK_SCHEDULE_PATH}/schedule_parallel_mr.${curr_time}

	mkdir -p ${user_project_dir}/log

	local task_name=""
	for task_name in `echo "${task_list}"`
	do
		local tmp_log_file=${MRFRAMEWORK_LOG_PATH}/schedule_${task_name}.err
  		local log_file=${user_project_dir}/log/schedule_mapreduce_${task_name}.err

  		echo "${task_name} ${tmp_log_file} ${log_file}" >>${schedule_parallel_task_file}
  
		schedule_mr_task ${task_name} ${log_file} 2>${tmp_log_file} &
	done

 	## 等待所有并行任务结束 ##
 	touch ${schedule_parallel_task_file}

 	local is_task_finished=0

 	while :
 	do
  		while read -r task_name tmp_log_file log_file
  		do
   			is_task_finished ${log_file}
   			local ret=$?

   			if [ ${ret} -eq ${JOB_RUNNING} ]
   			then
    				echo "${task_name} ${tmp_log_file} ${log_file}" >>${schedule_parallel_task_file}.running
   			elif [ ${ret} -eq ${JOB_SUCCEED} ]
   			then
    				rm -f ${tmp_log_file}
    				echo "${task_name}" >>${schedule_parallel_task_file}.succeed
   			elif [ ${ret} -eq ${JOB_FAILED} ]
   			then
    				rm -f ${tmp_log_file}
    				echo "${task_name}" >>${schedule_parallel_task_file}.failed
   			fi
  		done <${schedule_parallel_task_file}

  		touch ${schedule_parallel_task_file}.running
  		mv ${schedule_parallel_task_file}.running ${schedule_parallel_task_file}
  		if [ ! -s ${schedule_parallel_task_file} ]
  		then
   			is_task_finished=1
  		fi
                
  		if [ ${is_task_finished} -eq 1 ]
		then
			break
  		else
   			sleep 5
		fi
 	done

 	## 运行结果 ##
 	touch ${schedule_parallel_task_file}.succeed
 	while read -r task_name
 	do
  		echo "${task_name} schedule succeed" >&2
 	done <${schedule_parallel_task_file}.succeed
        
 	touch ${schedule_parallel_task_file}.failed
 	while read -r task_name
	do
		echo "${task_name} schedule failed" >&2
	done <${schedule_parallel_task_file}.failed

 	local ret=0
 	if [ -s ${schedule_parallel_task_file}.failed ]
 	then
  		ret=1
 	fi

 	rm -f ${schedule_parallel_task_file}
 	rm -f ${schedule_parallel_task_file}.succeed
 	rm -f ${schedule_parallel_task_file}.failed

	return ${ret}
}

function schedule_parallel_local_task()
{
	local task_list=$1
	local user_project_dir=$2

	local curr_time=`date +%Y%m%d_%H%M%S_%s%N`
	local schedule_parallel_task_file=${MRFRAMEWORK_SCHEDULE_PATH}/schedule_parallel_local.${curr_time}

	mkdir -p ${user_project_dir}/log

	local task_name=""
	for task_name in `echo "${task_list}"`
	do
  		local tmp_log_file=${MRFRAMEWORK_LOG_PATH}/schedule_${task_name}.err
  		local log_file=${user_project_dir}/log/schedule_local_${task_name}.err

  		echo "${task_name} ${tmp_log_file} ${log_file}" >>${schedule_parallel_task_file}
  
		schedule_local_task ${task_name} ${log_file} 2>${tmp_log_file} &
	done


 	## 等待所有并行任务结束 ##
 	touch ${schedule_parallel_task_file}

 	local is_task_finished=0

 	while :
 	do
  		while read -r task_name tmp_log_file log_file
  		do
   			is_task_finished ${tmp_log_file}
   			local ret=$?

   			if [ ${ret} -eq ${JOB_RUNNING} ]
   			then
				echo "${task_name} ${tmp_log_file} ${log_file}" >>${schedule_parallel_task_file}.running
   			elif [ ${ret} -eq ${JOB_SUCCEED} ]
   			then
				rm -f ${tmp_log_file}
				echo "${task_name}" >>${schedule_parallel_task_file}.succeed
			elif [ ${ret} -eq ${JOB_FAILED} ]
			then
				rm -f ${tmp_log_file}
				echo "${task_name}" >>${schedule_parallel_task_file}.failed
   			fi
  		done <${schedule_parallel_task_file}

  		touch ${schedule_parallel_task_file}.running
  		mv ${schedule_parallel_task_file}.running ${schedule_parallel_task_file}
  		if [ ! -s ${schedule_parallel_task_file} ]
  		then
   			is_task_finished=1
  		fi
                
  		if [ ${is_task_finished} -eq 1 ]
		then
			break
  		else
			sleep 5
		fi
 	done

 	## 运行结果 ##
 	touch ${schedule_parallel_task_file}.succeed
 	while read -r task_name
 	do
  		echo "${task_name} schedule succeed" >&2
 	done <${schedule_parallel_task_file}.succeed
        
 	touch ${schedule_parallel_task_file}.failed
 	while read -r task_name
	do
		echo "${task_name} schedule failed" >&2
	done <${schedule_parallel_task_file}.failed

 	local ret=0
 	if [ -s ${schedule_parallel_task_file}.failed ]
 	then
  		ret=1
 	fi

 	rm -f ${schedule_parallel_task_file}
 	rm -f ${schedule_parallel_task_file}.succeed
 	rm -f ${schedule_parallel_task_file}.failed

	return ${ret}
}

function is_task_finished()
{
 	local log_file=$1
 	local ret=${JOB_RUNNING}
                                
 	local is_succeed=`grep -ia "${JOB_SUCCEED_TAG}" ${log_file} 2>/dev/null`
 	if [ -n "${is_succeed}" ]
 	then
  		ret=${JOB_SUCCEED}
 	fi

 	local is_failed=`grep -Eia "${JOB_FAILED_TAG}" ${log_file} 2>/dev/null`
 	if [ -n "${is_failed}" ]
 	then
  		ret=${JOB_FAILED}
 	fi

 	return ${ret}
}

function is_mr_task_ok()
{
 	local task_index=$1

 	if [ -z "${MR_TASK_INPUT[$task_index]}" ]
 	then
  		echo "MR_TASK_INPUT[$task_index] is empty" >&2
  		return 0
 	fi

 	if [ -z "${MR_TASK_OUTPUT[$task_index]}" ]
 	then
 		echo "MR_TASK_OUTPUT[$task_index] is empty" >&2
  		return 0
 	fi

	local is_hit_protect=`is_output_dir_protected ${MR_TASK_OUTPUT[$task_index]}`
 	if [ ${is_hit_protect} -eq 1 ]
 	then
  		echo "Are you kidding me? What do you want to do? ............" >&2
  		return 0
 	fi  

 	if [ -z "${MR_TASK_MAPPER_CMD[$task_index]}" ]
 	then
  		echo "MR_TASK_MAPPER_CMD[$task_index] is empty" >&2
  		return 0
 	fi

 	local upload_files=`echo "${MR_TASK_UPLOAD_FILES[$task_index]}" | awk '{
  		n = split($0, arr, "[, \t]")
  		for (i = 1; i <= n; ++i)
  		{
   			if (length(arr[i]) > 0)
   			{
    				print arr[i]
   			}
  		}
 	}'`

 	for file_path in `echo "${upload_files}"`
 	do
  		if [ ! -f ${file_path} ]
  		then
   			echo "${file_path} not exist" >&2
   			return 0
  		fi
 	done

 	if [ -n "${MR_TASK_CONFIG_FILE[$task_index]}" ]
 	then
  		if [ ! -f ${MR_TASK_CONFIG_FILE[$task_index]} ]
  		then
   			echo "${MR_TASK_CONFIG_FILE[$task_index]} not exist" >&2
   			return 0
  		fi
 	fi

 	local ext_jars=`echo "${MR_TASK_EXT_JARS[$task_index]}" | awk '{
  		n = split($0, arr, "[, \t]")
  		for (i = 1; i <= n; ++i)
  		{
   			if (length() > 0)
   			{
    				print arr[i]
   			}
  		}
 	}'`
 	for file_path in `echo "${ext_jars}"`
 	do
  		if [ ! -f ${file_path} ]
  		then
   			echo "${file_path} not exist" >&2
   			return 0
  		fi
 	done

 	return 1
}

function construct_mr_task_cmd()
{
 	local task_index=$1

 	local input_opt=`echo "${MR_TASK_INPUT[$task_index]}" | awk 'BEGIN{
  		input_opt = ""
 	}{
  		n = split($0, arr, "[, \t]")
  		for (i = 1; i <= n; ++i)
  		{
   			if (length(arr[i]) > 0)
   			{
    				input_opt = input_opt" -input "arr[i]
   			}
  		}
 	}END{
  		print input_opt
 	}'`

 	local output_opt="-output ${MR_TASK_OUTPUT[$task_index]}"
 	local mapper_opt="-mapper \"${MR_TASK_MAPPER_CMD[$task_index]}\""
 	local reducer_opt=""
 	if [ -n "${MR_TASK_REDUCER_CMD[$task_index]}" ]
 	then
  		reducer_opt="-reducer \"${MR_TASK_REDUCER_CMD[$task_index]}\""
 	fi

 	local file_opt=`echo "${MR_TASK_UPLOAD_FILES[$i]}" | awk 'BEGIN{
  		file_opt = ""                
 	}                
 	{
  		n = split($0, arr, "[, \t]")
  		for (i = 1; i <= n; ++i)
  		{
   			if (length(arr[i]) > 0)
   			{
    				file_opt = file_opt" -file "arr[i]
   			}
  		}                
 	}END{
  		print file_opt
 	}'`
 
 	local job_conf_opt=""
 	if [ -n "${MR_TASK_CONFIG_FILE[$task_index]}" ]
 	then
  		job_conf_opt=`parse_job_config_file ${MR_TASK_CONFIG_FILE[$task_index]}`
		job_conf_opt="${job_conf_opt} -jobconf mapred.job.name=${MR_TASK_NAME[$task_index]}"
 	fi

 	local input_format_opt=""
 	if [ -n "${MR_TASK_INPUT_FORMAT[$task_index]}" ]
 	then
  		input_format_opt="-inputformat ${MR_TASK_INPUT_FORMAT[$task_index]}"
 	fi

 	local output_format_opt=""
 	if [ -n "${MR_TASK_OUTPUT_FORMAT[$task_index]}" ]
 	then
  		output_format_opt="-outputformat ${MR_TASK_OUTPUT_FORMAT[$task_index]}"
 	fi

 	local partitioner=""
 	if [ -n "${MR_TASK_PARTITIONER[$task_index]}" ]
 	then
  		partitioner="-partitioner ${MR_TASK_PARTITIONER[$task_index]}"
 	fi

 	local libjars_opt=`echo "${MR_TASK_EXT_JARS[$i]}" | awk 'BEGIN{
                libjars_opt = ""                
        }                
        {
                n = split($0, arr, "[, \t]")
                for (i = 1; i <= n; ++i)
                {
                        if (length(arr[i]) > 0)
                        {
                                libjars_opt = libjars_opt" -libjars "arr[i]
                        }
                }                
        }END{
                print libjars_opt
        }'`

 	local cmd="${HADOOP_BIN} jar ${HADOOP_STREAMING_JAR_PATH} ${libjars_opt} ${input_opt} ${output_opt} ${mapper_opt} ${reducer_opt} ${file_opt} ${job_conf_opt} ${input_format_opt} ${output_format_opt} ${partitioner}"
 	echo "${cmd}"

 	return 0
}

function parse_job_config_file()
{
 	local job_config_file=$1

	${JAVA_HOME}/bin/java -Djava.ext.dirs=${MRFRAMEWORK_LIB_PATH} \
		com.github.panhongan.util.hadoop.MapReduceTaskXmlParser ${job_config_file}
 
 	return 0
}


function check_task_type()
{
 	local task_name=$1
 	local task_type=$2

 	if [ "${task_type}" == "${TASK_TYPE_MAPREDUCE}" ]
 	then
		for((i = 0; i < ${MR_TASK_NUM};++i))
  		do
   			if [ "${task_name}" == "${MR_TASK_NAME[$i]}" ]
   			then
    				echo "${TASK_TYPE_MAPREDUCE}"
    				return 0
   			fi
  		done
 	elif [ "${task_type}" == "${TASK_TYPE_PARALLEL_MAPREDUCE}" ]
 	then
  		for((i = 0; i < ${MR_TASK_NUM};++i))
  		do
   			if [ "${task_name}" == "${MR_TASK_NAME[$i]}" ]
   			then
    				echo "${TASK_TYPE_PARALLEL_MAPREDUCE}"
    				return 0
   			fi
  		done
 	elif [ "${task_type}" == "${TASK_TYPE_LOCAL}" ]
 	then
  		for((i = 0; i < ${LOCAL_TASK_NUM}; ++i))
  		do
   			if [ "${task_name}" == "${LOCAL_TASK_NAME[$i]}" ]
   			then
    				echo "${TASK_TYPE_LOCAL}"
    				return 0
   			fi
  		done
	elif [ "${task_type}" == "${TASK_TYPE_PARALLEL_LOCAL}" ]
	then
		for((i = 0; i < ${LOCAL_TASK_NUM}; ++i))
		do
			if [ "${task_name}" == "${LOCAL_TASK_NAME[$i]}" ]
			then
				echo "${TASK_TYPE_PARALLEL_LOCAL}"
				return 0
			fi
		done
 	fi

 	echo ""
 	return 1
}

function is_valid_task_type()
{
 	local task_type=$1

 	if [ "${task_type}" == "${TASK_TYPE_LOCAL}" -o \
		"${task_type}" == "${TASK_TYPE_MAPREDUCE}" -o \
		"${task_type}" == "${TASK_TYPE_PARALLEL_MAPREDUCE}" -o \
		"${task_type}" == "${TASK_TYPE_PARALLEL_LOCAL}" ]
 	then
  		echo 1
 	else
  		echo 0
 	fi

 	return 0
}

function save_breakpoint()
{
 	local breakpoint_task_type=$1
 	local breakpoint_task=$2
 	local breakpoint_file=$3

 	echo "${breakpoint_task_type} ${breakpoint_task}" >${breakpoint_file}

 	return 0
}

function format_by_space()
{
 	local src_str=$1

 	echo "${src_str}" | awk '{
  		str = $1
  		for (i = 2; i <= NF; ++i)
  		{
   			str = str" "$i
  		}
  		print str
 	}'

 	return 0
}

function is_output_dir_protected()
{
	local output_dir=$1

	echo "${output_dir}" | awk 'BEGIN{
		while ((getline < "'${MRFRAMEWORK_CONF_PATH}'/protect.conf") > 0)
		{
			protect_dir[$1]
		}
		close("'${MRFRAMEWORK_CONF_PATH}'/protect.conf")

		hdfs_path = ""
	}{
		sub("^hdfs://[0-9a-zA-Z.:]+", "", $1)	# hdfs://*:*
		sub("[/]+$", "", $1)
		n = split($1, arr, "/")
		for (i = 1; i < n; ++i) 		# i < n
		{
			if (length(arr[i]) > 0)
			{
				hdfs_path = hdfs_path"/"arr[i]
			}
		}

		if (hdfs_path == "")
                {
                        hdfs_path = "/"
                }

                if (hdfs_path in protect_dir)
                {
                        print 1
                }
                else
                {
                        print 0
                }
	}'

	return 0
}

prepare
