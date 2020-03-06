from __future__ import print_function

import sys

import numpy as np
from pyspark.sql import SparkSession
#查詢最大值最小值
# select 't_admin_audit' as tableNm  ,min(id)  as lowerBound,max(id)as upperBound, 1 as index    from  t_admin_audit 
# union all
# select 't_auto_review_loan' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 2 as index    from  t_auto_review_loan
# union all
# select 't_bankcard' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 3 as index    from  t_bankcard
# union all
# select 't_channel_details' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 4 as index    from  t_channel_details
# union all
# select 't_clear_detail_log' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 5 as index    from  t_clear_detail_log 
# union all
# select 't_code' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 6 as index    from  t_code 
# union all
# select 't_collection_audit' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 7 as index    from  t_collection_audit 
# union all
# select 't_collection_blacklist' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 8 as index    from  t_collection_blacklist 
# union all
# select 't_contact' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 9 as index    from  t_contact 
# union all
# select 't_coupon_send' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 10 as index    from  t_coupon_send 
# union all
# select 't_customer' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 11 as index    from  t_customer 
# union all
# select 't_customer_app' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 12 as index    from  t_customer_app 
# union all
# select 't_customer_device_info' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 13 as index    from  t_customer_device_info 
# union all
# select 't_customer_install_info' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 14 as index    from  t_customer_install_info 
# union all
# select 't_customer_tag_rel' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 15 as index    from  t_customer_tag_rel 
# union all
# select 't_date_range' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 16 as index    from  t_date_range 
# union all
# select 't_employment' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 17 as index    from  t_employment 
# union all
# select 't_engine_rule_detail' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 18 as index    from  t_engine_rule_detail 
# union all
# select 't_file' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 19 as index    from  t_file 
# union all
# select 't_invitation' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 20 as index    from  t_invitation 
# union all
# select 't_job_metric' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 21 as index    from  t_job_metric 
# union all
# select 't_loan_app' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 22 as index    from  t_loan_app 
# union all
# select 't_loan_app_review_summary' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 23 as index    from  t_loan_app_review_summary 
# union all
# select 't_loan_app_status_log' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 24 as index    from  t_loan_app_status_log 
# union all
# select 't_loan_issue' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 25 as index    from  t_loan_issue 
# union all
# select 't_loan_log' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 26 as index    from  t_loan_log 
# union all
# select 't_loan_tag_rel' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 27 as index    from  t_loan_tag_rel 
# union all
# select 't_lpay' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 28 as index    from  t_lpay  
# union all
# select 't_lpay_deposit' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 29 as index    from  t_lpay_deposit 
# union all
# select 't_message' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 30 as index    from  t_message 
# union all
# select 't_personal_info' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 31 as index    from  t_personal_info  
# union all
# select 't_record_bankcard' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 32 as index    from  t_record_bankcard  
# union all
# select 't_record_contact' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 33 as index    from  t_record_contact  
# union all
# select 't_record_employment' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 34 as index    from  t_record_employment  
# union all
# select 't_record_file' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 35 as index    from  t_record_file 
# union all
# select 't_record_personal_info' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 36 as index    from  t_record_personal_info  
# union all
# select 't_review_blacklist' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 37 as index    from  t_review_blacklist  
# union all
# select 't_report_description' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 38 as index    from  t_report_description  
# union all
# select 't_review_step_execution' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 39 as index    from  t_review_step_execution  
# union all
# select 't_send_coupon_rel' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 40 as index    from  t_send_coupon_rel  
# union all
# select 't_sms' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 41 as index    from  t_sms  
# union all
# select 't_thirdparty_data' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 42 as index    from  t_thirdparty_data   
# union all
# select 't_tongdun_data' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 43 as index    from  t_tongdun_data    
# union all
# select 't_virtual_account' as tableNm , min(id) as lowerBound ,max(id) as upperBound , 44 as index    from  t_virtual_account  



# t_whitelist_old_customer没Id
#############################################################################################################################
# builtins
# __main__
if __name__ == "__main__":
    tableList=[
	             {"tablenm":"t_admin_audit", "lowerbound":"100000", "upperbound":"44950617", "index":1},
				 {"tablenm":"t_auto_review_loan", "lowerbound":"1000000", "upperbound":"8634166", "index":2},
				 {"tablenm":"t_bankcard", "lowerbound":"100000", "upperbound":"7589597", "index":3},
				 {"tablenm":"t_channel_details", "lowerbound":"122098", "upperbound":"9655636", "index":4},
				 {"tablenm":"t_clear_detail_log", "lowerbound":"100000", "upperbound":"295709", "index":5},
				 {"tablenm":"t_code", "lowerbound":"1", "upperbound":"252670", "index":6},
				 {"tablenm":"t_collection_audit", "lowerbound":"100000", "upperbound":"24971782", "index":7},
				 {"tablenm":"t_collection_blacklist", "lowerbound":"100000", "upperbound":"100865", "index":8},
				 {"tablenm":"t_contact", "lowerbound":"100000", "upperbound":"25352795", "index":9},
				 {"tablenm":"t_coupon_send", "lowerbound":"100000", "upperbound":"161297", "index":10},
				 {"tablenm":"t_customer", "lowerbound":"10001", "upperbound":"3959039", "index":11},
				 {"tablenm":"t_customer_app", "lowerbound":"100000", "upperbound":"180688", "index":12} ,
				 {"tablenm":"t_customer_device_info", "lowerbound":"100000", "upperbound":"3225412", "index":13} ,
				 {"tablenm":"t_customer_install_info", "lowerbound":"100005", "upperbound":"4512404", "index":14} ,
				 {"tablenm":"t_customer_tag_rel", "lowerbound":"100000", "upperbound":"713817", "index":15},
				 {"tablenm":"t_date_range", "lowerbound":"1", "upperbound":"73201", "index":16},
				 {"tablenm":"t_employment", "lowerbound":"100000", "upperbound":"7590330", "index":17},
				 {"tablenm":"t_engine_rule_detail", "lowerbound":"2111776", "upperbound":"28654512", "index":18},
				 {"tablenm":"t_file", "lowerbound":"100000", "upperbound":"37804606", "index":19},
				 {"tablenm":"t_invitation", "lowerbound":"100000", "upperbound":"395692", "index":20} ,
				 {"tablenm":"t_job_metric", "lowerbound":"1", "upperbound":"13528582", "index":21},
				 {"tablenm":"t_loan_app", "lowerbound":"20001", "upperbound":"7652032", "index":22},
				 {"tablenm":"t_loan_app_review_summary", "lowerbound":"100000", "upperbound":"7090823", "index":23},
				 {"tablenm":"t_loan_app_status_log", "lowerbound":"100000", "upperbound":"40966319", "index":24},
				 {"tablenm":"t_loan_issue", "lowerbound":"100000", "upperbound":"3859163", "index":25},
				 {"tablenm":"t_loan_log", "lowerbound":"100000", "upperbound":"103054", "index":26},
				 {"tablenm":"t_loan_tag_rel", "lowerbound":"100000", "upperbound":"6004282", "index":27},
				 {"tablenm":"t_lpay", "lowerbound":"30001", "upperbound":"3555601", "index":28},
				 {"tablenm":"t_lpay_deposit", "lowerbound":"100000", "upperbound":"3340075", "index":29},
				 {"tablenm":"t_message", "lowerbound":"100000", "upperbound":"40276661", "index":30},
				 {"tablenm":"t_personal_info", "lowerbound":"100000", "upperbound":"7590359", "index":31},
				 {"tablenm":"t_record_contact", "lowerbound":"100000", "upperbound":"9148761", "index":33},
				 {"tablenm":"t_record_employment", "lowerbound":"100000", "upperbound":"2984007", "index":34},
				 {"tablenm":"t_record_file", "lowerbound":"100000", "upperbound":"8501137", "index":35},
				 {"tablenm":"t_record_personal_info", "lowerbound":"100000", "upperbound":"3191957", "index":36},
				 {"tablenm":"t_review_blacklist", "lowerbound":"100000", "upperbound":"104164", "index":37},
				 {"tablenm":"t_review_step_execution", "lowerbound":"100000", "upperbound":"14048795", "index":39},
				 {"tablenm":"t_send_coupon_rel", "lowerbound":"1", "upperbound":"48319", "index":40},
				 {"tablenm":"t_sms", "lowerbound":"1", "upperbound":"44127952", "index":41},
				 {"tablenm":"t_thirdparty_data", "lowerbound":"100003", "upperbound":"3721870", "index":42},
				 {"tablenm":"t_tongdun_data", "lowerbound":"100000", "upperbound":"903446", "index":43},
				 {"tablenm":"t_virtual_account", "lowerbound":"100000", "upperbound":"1889199", "index":44},
				]
						# {"tablenm":"t_report_description", "lowerbound":NULL, "upperbound":NULL, "index":38}
						#	 {"tablenm":"t_record_bankcard", "lowerbound":NULL, "upperbound":NULL, "index":32}
    spark = SparkSession\
        .builder\
        .appName("Python Demo")\
        .enableHiveSupport()\
        .getOrCreate()

    url = "jdbc:postgresql://pgm-d9jkp19xeyji7eatjo.pgsql.ap-southeast-5.rds.aliyuncs.com:3433/banda"
    user= "postgres"
    password="vHeqPwro2HriV7CrM8Wh"
    for row in tableList:
        tablenm=row["tablenm"]
        lowerbound=row["lowerbound"]
        upperbound=row["upperbound"]
        df = spark.read.format("jdbc").option("url", url).option("user", user).option("password", password).option("partitionColumn", "id").option("dbtable", tablenm).option("lowerBound", lowerbound).option("upperBound", upperbound).option("numPartitions", 1000).load()
        df.createOrReplaceTempView("t")
        bound = spark.sql("select max(id) max, min(id) min from t")
        max = bound.collect()[0].max
        min = bound.collect()[0].min
        df_table = spark.read.format("jdbc").option("url", url).option("dbtable",  tablenm).option("user", user).option("password", password).option("partitionColumn", "id").option("lowerBound", min).option("upperBound", max).option("numPartitions", 1000).load()
#         示例目录
        path="s3://rupiahplus-data-warehouse/aliyun/banda/"+tablenm
        df_table.write.mode("overwrite").orc(path)
    spark.stop()
