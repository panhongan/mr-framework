#!/bin/bash

function start()
{
	source ../conf/project.conf

	${MR_FRAMEWORK_BIN} ${LOCAL_CONF_PATH}/framework.conf
	
	return 0
}

start


