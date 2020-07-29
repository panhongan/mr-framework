###################################################################
author: phadyc@126.com

###################################################################
version: mr-framework 1.0.0 (2014-08-01)
增加功能点:
1. 支持定制master，串行map-reduce任务，并行map-reduce任务的执行顺序
2. 配置各个任务及参数

调用方式: sh -x mr-framework/bin/start-mr.sh <framework.conf>
###################################################################



###################################################################
version: mr-framework 1.0.1 (2014-08-16)
增加功能点:
1. 增加断点功能

调用方式: sh -x mr-framework/bin/start-mr.sh <framework.conf> [ -b [<breakpoint_task>] ]
-b 		: 	断点标识, 为空则从头开始运行
breakpoint_task : 	断点开始的任务，如果为空，自动从上一次失败的任务开始运行
###################################################################



###################################################################
version: mr-framework 1.0.2 (2014-09-30)
增加功能点:
1. -input 支持多个输入路径
2. 支持 -libjars 参数

调用方式: sh -x mr-framework/bin/start-mr.sh <framework.conf> [ -b [<breakpoint_task>] ]
###################################################################



###################################################################
version: mr-framework 1.0.3 (2014-11-12)
修复bug:
1. 支持<framework.conf>为相对路径时

调用方式: sh -x mr-framework/bin/start-mr.sh <framework.conf> [ -b [<breakpoint_task>] ]



####################################################################
version: mr-framework 1.0.4 (2015-03-20)
增加功能点:
1. 增加保护集群目录

修复bug:
1. exit时的不完整

调用方式: sh -x mr-framework/bin/start-mr.sh <framework.conf> [ -b [<breakpoint_task>] ]



#####################################################################
version: mr-framework 1.0.5 (2015-12-03)
增加功能点:
1. 增加 Parallel_Local 运行模式。增强了任务实现的灵活性。
 
调用方式: sh -x mr-framework/bin/start-mr.sh <framework.conf> [ -b [<breakpoint_task>] ]

