#! /bin/env python
#-*- coding:utf8 -*-

import sys
import os
import time
import datetime
import socket
import yaml
import requests
import json

from mongodb_server import mongodbMonitor


falcon_client = "http://127.0.0.1:1988/v1/push"
ts = int(time.time())

# all falcon counter type metrics list

mongodb_counter_metric = ["asserts_msg",
                "asserts_regular",
                "asserts_rollovers",
                "asserts_user",
                "asserts_warning",
                "page_faults",
		"connections_totalCreated",
                "locks_Global_acquireCount_ISlock",
                "locks_Global_acquireCount_IXlock",
                "locks_Global_acquireCount_Slock",
                "locks_Global_acquireCount_Xlock",
                "locks_Global_acquireWaitCount_ISlock",
                "locks_Global_acquireWaitCount_IXlock",
                "locks_Global_timeAcquiringMicros_ISlock",
                "locks_Global_timeAcquiringMicros_IXlock",
                "locks_Database_acquireCount_ISlock",
                "locks_Database_acquireCount_IXlock",
                "locks_Database_acquireCount_Slock",
                "locks_Database_acquireCount_Xlock",
                "locks_Collection_acquireCount_ISlock",
                "locks_Collection_acquireCount_IXlock",
                "locks_Collection_acquireCount_Xlock",
                "opcounters_command",
                "opcounters_insert",
                "opcounters_delete",
                "opcounters_update",
                "opcounters_query",
                "opcounters_getmore",
                "opcountersRepl_command",
                "opcountersRepl_insert",
                "opcountersRepl_delete",
                "opcountersRepl_update",
                "opcountersRepl_query",
                "opcountersRepl_getmore",
                "network_bytesIn",
                "network_bytesOut",
                "network_numRequests",
                "backgroundFlushing_flushes",
                "backgroundFlushing_last_ms",
                "cursor_timedOut",
                "wt_cache_readinto_bytes",
                "wt_cache_writtenfrom_bytes",
                "wt_bm_bytes_read",
                "wt_bm_bytes_written",
                "wt_bm_blocks_read",
                "wt_bm_blocks_written"]


f=open("../conf/mongomon.conf")
y = yaml.load(f)
f.close()
mongodb_items = y["items"]

for mongodb_ins in mongodb_items:

        mongodb_monitor = mongodbMonitor()
	
        mongodb_tag = "mongo=" + str(mongodb_ins["port"])

        err,conn = mongodb_monitor.mongodb_connect(host=mongodb_ins["ip"],port=mongodb_ins["port"], user=mongodb_ins["user"], password=mongodb_ins["password"])
 
	mongodb_upate_list = [] 
        if err != 0:
		key_item_dict =  {"endpoint": mongodb_ins["ip"], "metric": "mongo_local_alive", "tags":mongodb_tag , "timestamp":ts, "value": 0, "step": 60, "counterType": "GAUGE"}
		mongodb_upate_list.append(key_item_dict)
		r = requests.post(falcon_client,data=json.dumps(mongodb_upate_list))
		continue   #The instance is dead. upload the "mongo_alive_local=0" key, then continue.

        mongodb_dict = mongodb_monitor.get_mongo_monitor_data(conn)
        mongodb_dict_keys = mongodb_dict.keys()
	
        for mongodb_metric in mongodb_dict_keys:

                if mongodb_metric in mongodb_counter_metric :
                        key_item_dict = {"endpoint": mongodb_ins["ip"], "metric": mongodb_metric, "tags":mongodb_tag , "timestamp":ts, "value": mongodb_dict[mongodb_metric], "step": 60, "counterType": "COUNTER"}
                else:
                        key_item_dict =  {"endpoint": mongodb_ins["ip"], "metric": mongodb_metric, "tags":mongodb_tag , "timestamp":ts, "value": mongodb_dict[mongodb_metric], "step": 60, "counterType": "GAUGE"}

                mongodb_upate_list.append(key_item_dict)
	r = requests.post(falcon_client,data=json.dumps(mongodb_upate_list))
