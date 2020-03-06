from __future__ import print_function

import sys

import numpy as np
from pyspark.sql import SparkSession
#查詢最大值最小值
# select 't_admin_audit' as tablenm  ,min(id)  as minid,max(id)as maxid  from t_admin_audit
# union all
# select 't_auto_review_loan' as tablenm , min(id) as minid ,max(id) as maxid   from t_auto_review_loan
# union all
# select 't_bankcard' as tablenm , min(id) as minid ,max(id) as maxid   from t_bankcard
# union all
# select 't_channel_details' as tablenm , min(id) as minid ,max(id) as maxid   from t_channel_details
# union all
# select 't_collection_audit' as tablenm , min(id) as minid ,max(id) as maxid   from t_collection_audit
# union all
# select 't_collection_blacklist' as tablenm , min(id) as minid ,max(id) as maxid   from t_collection_blacklist
# union all
# select 't_contact' as tablenm , min(id) as minid ,max(id) as maxid   from t_contact
# union all
# select 't_customer' as tablenm , min(id) as minid ,max(id) as maxid   from t_customer
# union all
# select 't_employment' as tablenm , min(id) as minid ,max(id) as maxid   from t_employment
# union all
# select 't_engine_rule_detail' as tablenm , min(id) as minid ,max(id) as maxid   from t_engine_rule_detail
# union all
# select 't_loan_app' as tablenm , min(id) as minid ,max(id) as maxid   from t_loan_app
# union all
# select 't_loan_app_review_summary' as tablenm , min(id) as minid ,max(id) as maxid   from t_loan_app_review_summary
# union all
# select 't_loan_app_status_log' as tablenm , min(id) as minid ,max(id) as maxid   from t_loan_app_status_log
# union all
# select 't_loan_issue' as tablenm , min(id) as minid ,max(id) as maxid   from t_loan_issue
# union all
# select 't_lpay' as tablenm , min(id) as minid ,max(id) as maxid   from t_lpay
# union all
# select 't_lpay_deposit' as tablenm , min(id) as minid ,max(id) as maxid   from t_lpay_deposit
# union all
# select 't_message' as tablenm , min(id) as minid ,max(id) as maxid   from t_message
# union all
# select 't_personal_info' as tablenm , min(id) as minid ,max(id) as maxid   from t_personal_info
# union all
# select 't_record_bankcard' as tablenm , min(id) as minid ,max(id) as maxid   from t_record_bankcard
# union all
# select 't_record_contact' as tablenm , min(id) as minid ,max(id) as maxid   from t_record_contact
# union all
# select 't_record_employment' as tablenm , min(id) as minid ,max(id) as maxid   from t_record_employment
# union all
# select 't_record_file' as tablenm , min(id) as minid ,max(id) as maxid   from t_record_file
# union all
# select 't_record_personal_info' as tablenm , min(id) as minid ,max(id) as maxid   from t_record_personal_info
# union all
# select 't_review_blacklist' as tablenm , min(id) as minid ,max(id) as maxid   from t_review_blacklist
# union all
# select 't_review_step_execution' as tablenm , min(id) as minid ,max(id) as maxid   from t_review_step_execution
# union all
# select 't_sms' as tablenm , min(id) as minid ,max(id) as maxid   from t_sms
# union all
# select 't_virtual_account' as tablenm , min(id) as minid ,max(id) as maxid   from t_virtual_account
#############################################################################################################################
# builtins
# __main__
if __name__ == "__main__":
    tableList=[{"tableNm":"t_admin_audit","lowerBound":100000,"upperBound":1003035,"index":1},
               {"tableNm":"t_auto_review_loan","lowerBound":1000000,"upperBound":1212324,"index":2},
               {"tableNm":"t_bankcard","lowerBound":100000,"upperBound":162475,"index":3},
               {"tableNm":"t_channel_details","lowerBound":100000,"upperBound":329424,"index":4},
               {"tableNm":"t_collection_audit","lowerBound":100000,"upperBound":550252,"index":5},
               {"tableNm":"t_collection_blacklist","lowerBound":100000,"upperBound":100043,"index":6},
               {"tableNm":"t_contact","lowerBound":100000,"upperBound":685789,"index":7},
               {"tableNm":"t_customer","lowerBound":100000,"upperBound":299932,"index":8},
               {"tableNm":"t_employment","lowerBound":100000,"upperBound":295244,"index":9},
               {"tableNm":"t_engine_rule_detail","lowerBound":100000,"upperBound":778422,"index":10},
               {"tableNm":"t_loan_app","lowerBound":100000,"upperBound":295246,"index":11},
               {"tableNm":"t_loan_app_review_summary","lowerBound":100000,"upperBound":283451,"index":12},
               {"tableNm":"t_loan_app_status_log","lowerBound":100000,"upperBound":949215,"index":13},
               {"tableNm":"t_loan_issue","lowerBound":100000,"upperBound":132028,"index":14},
               {"tableNm":"t_lpay","lowerBound":100000,"upperBound":135638,"index":15},
               {"tableNm":"t_lpay_deposit","lowerBound":100000,"upperBound":125846,"index":16},
               {"tableNm":"t_message","lowerBound":100000,"upperBound":1073201,"index":17},
               {"tableNm":"t_personal_info","lowerBound":100000,"upperBound":295248,"index":18},
               {"tableNm":"t_record_bankcard","lowerBound":100000,"upperBound":154754,"index":19},
               {"tableNm":"t_record_contact","lowerBound":100000,"upperBound":554550,"index":20},
               {"tableNm":"t_record_employment","lowerBound":100000,"upperBound":257024,"index":21},
               {"tableNm":"t_record_file","lowerBound":100000,"upperBound":375658,"index":22},
               {"tableNm":"t_record_personal_info","lowerBound":100000,"upperBound":269711,"index":23},
               {"tableNm":"t_review_blacklist","lowerBound":100000,"upperBound":100100,"index":24},
               {"tableNm":"t_review_step_execution","lowerBound":100000,"upperBound":441439,"index":25},
               {"tableNm":"t_sms","lowerBound":1,"upperBound":979041,"index":26},
               {"tableNm":"t_virtual_account","lowerBound":100000,"upperBound":100003,"index":27}
        
    ]
    spark = SparkSession\
        .builder\
        .appName("Python Demo")\
        .enableHiveSupport()\
        .getOrCreate()

    url = "jdbc:postgresql://luzon-prod.ct3lmpqmpvcq.ap-southeast-1.rds.amazonaws.com:5432/luzon"
    user= "luzon"
    password="RSFH0vpjp88pmB"
    for row in tableList:
        tableNm=row["tableNm"]
        lowerBound=row["lowerBound"]
        upperBound=row["upperBound"]
        df = spark.read.format("jdbc").option("url", url).option("user", user).option("password", password).option("partitionColumn", "id").option("dbtable", tableNm).option("lowerBound", lowerBound).option("upperBound", upperBound).option("numPartitions", 1000).load()
        df.createOrReplaceTempView("t")
        bound = spark.sql("select max(id) max, min(id) min from t")
        max = bound.collect()[0].max
        min = bound.collect()[0].min
        df_table = spark.read.format("jdbc").option("url", url).option("dbtable",  tableNm).option("user", user).option("password", password).option("partitionColumn", "id").option("lowerBound", min).option("upperBound", max).option("numPartitions", 1000).load()
        path="s3://rupiahplus-data-warehouse/aliyun/luzon_emr/"+tableNm
        df_table.write.mode("overwrite").orc(path)
    spark.stop()
