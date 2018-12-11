#!/bin/sh

python udp_server.py 5000  sleep 5s &
python udp_server.py 5001 sleep 5s &
python udp_server.py 5002 sleep 5s &
python udp_server.py 5003 sleep 5s &
python test_client.py
# pid_list = $(ps -ef | grep python)
# for pid in pid_list; do
#     echo $pid
# done