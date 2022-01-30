#!/usr/bin/bash

proc_count=`ps -ef|grep acc_monitor.py|grep -v grep|wc -l`
echo `date` >> /opt/acc_monitor/process_monitor.log
if [ $proc_count == 1 ];then 
	echo "acc_monitor.py is running" >> /opt/acc_monitor/process_monitor.log
else 
	echo "acc_monitor.py is NOT running. restarting process" >> /opt/acc_monitor/process_monitor.log
	cd /opt/acc_monitor; nohup ./acc_monitor.py &
fi