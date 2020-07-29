# mr-framework
hadoop streaming job scheduler framework

##################################################################
mr-framework框架部署环境配置:

* conf/mrframework.conf
[1] JAVA_HOME

* conf/hadoop.conf
[1] HADOOP_BIN
[2] HADOOP_STREAMING_JAR_PATH

* conf/protect.conf
配置待保护的集群目录
###################################################################



###################################################################
应用接入: sh -x mr-framework/bin/start-mr.sh <user_framework_conf_file> [-b [<breakpoint_task>]]
参考示例：demo/bin/start.sh
