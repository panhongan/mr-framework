
###################################################################
环境配置:

* conf/mrframework.conf
[1]. JAVA_HOME


* conf/hadoop.conf
[1]. HADOOP_BIN
[2]. HADOOP_STREAMING_JAR_PATH


* conf/protect.conf
配置待保护的集群目录

###################################################################



###################################################################
使用方法:
sh -x mr-framework/bin/start.sh <framework.conf> [ -b [<breakpoint_task>] ]


