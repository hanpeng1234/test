# from __future__ import print_function

import sys

import numpy as np
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as F

import pytz
#北京时间0点半执行，相当于utc时间昨天的16:30 ，所以时间不-1天
# print(datetime.now(pytz.timezone("Asia/Shanghai")).strftime( '%Y-%m-%d'))
yesterday = datetime.now().strftime('%Y-%m-%d')
partitionlist=yesterday.split("-",3)
beforeyesterday = (datetime.now() + timedelta(-1)).strftime('%Y-%m-%d')
#防止当天出错之后,八点之后运行会丢失分区
partitionlist.append(beforeyesterday.split("-",3)[2])
dbmap = {"banda_guest": "1"}
#是否特殊special，特殊的需要自己写sql
guestTable = {"thirdparty":["select thirdparty_id, update_time, retry_times, create_time, regexp_replace(response, '(\d*)(8|62)(\d{4})(\d*)', '$1$2xxxx$4') response,channel,id,customer_id,loan_id,status from "," where client_id = 0"],
              "t_customer":"",
              "t_loan_app":  "",
              "t_loan_app_status_log": "",
              "t_lpay":  "",
              "t_personal_info":["select id, customer_id, loan_app_id, gender, credential_type, province, city, district, area, address, last_education, marital_status, children_number, residence_duration, facebook_id, car_number, house_number, ethnic, religion, nationality, ktp_gender, ktp_province, ktp_city, ktp_district, ktp_date_of_birth, ktp_analyze_status, status, create_time, update_time, email, cast(substring(credential_no, 11, 2) as bigint) birth_year, case when cast(substring(credential_no, 11, 2) as bigint) > 37 then (year(current_date()) - cast(substring(credential_no, 11, 2) as bigint) - 1900) else (year(current_date()) - cast(substring(credential_no, 11, 2) as bigint) - 2000) end age  from"],
              "t_auto_review_loan":"",
              "t_loan_app_review_summary":  "",
              "t_review_blacklist": "",
              "t_review_step_execution": "",
              "t_lpay_deposit": "",
              "t_engine_rule_detail":  "",
              "t_thirdparty_data": ["select * FROM "," WHERE data_type IN (10, 5, 8)"],
              "t_employment": "",
              "t_contact":  ["select ct.id, ct.customer_id, ct.loan_app_id, ct.name, sha2(ct.format_new_mobile, 256) mobile, ct.relation, ct.credential_no, ct.credential_type, ct.address, ct.company_name, ct.job_title, ct.create_time, ct.update_time, ct.comment, ct.mobile_status, cm.id contact_customer_id from ( SELECT tmp.id, tmp.customer_id, tmp.loan_app_id, tmp.name, tmp.mobile, tmp.relation, tmp.credential_no, tmp.credential_type, tmp.address, tmp.company_name, tmp.job_title, tmp.create_time, tmp.update_time, tmp.comment, tmp.mobile_status, CASE WHEN tmp.new_mobile LIKE '62%' THEN '0' || substring(tmp.new_mobile, 3, length(tmp.new_mobile)) WHEN tmp.new_mobile LIKE '8%' THEN '0' || tmp.new_mobile ELSE tmp.new_mobile END format_new_mobile FROM (SELECT *, regexp_replace(mobile, '[^0-9]', '') new_mobile FROM",") as tmp ) ct left join "," cm on (ct.format_new_mobile = cm.mobile)"],
              "t_collection_audit": "",
              "t_channel_details": "",
              "t_loan_tag_rel": "",
              "t_loan_log":  "",
              "t_record_personal_info": "",
              "t_customer_app":"",
              "t_customer_device_info": "",
              "t_customer_install_info":"",
			  "t_login_log":"",
              }
# print(__name__)builtins
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Demo") \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.driver.maxResultSize", "4g") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict");
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    databases = spark.sql("show databases")
    databases = databases.collect()
    for row in databases:
        databaseName = row["databaseName"]
        if databaseName != "" and databaseName in dbmap:
#             print(databaseName)
            databasesql = "show tables in " + databaseName
            tables = spark.sql(databasesql)
            tablelist = tables.collect();
            for row in tablelist:
                tableName = row["tableName"]
#                 print(tableName)
                if tableName !="" and tableName in guestTable:
                    if tableName=="thirdparty":
                        sql=guestTable[tableName][0]+" thirdparty_etl.thirdparty_t_task "+guestTable[tableName][1]
                        guestPath = "s3://rupiahplus-data-warehouse/stream/" + databaseName + "_etl/" + tableName
                        spark.sql(sql).write.mode("overwrite").orc(guestPath)
                    else :
                        #先给banda加上今天的partitions
                        tempDataBase = " banda_stream_etl_daily"
                        guestPath = "s3://rupiahplus-data-warehouse/stream/" + databaseName + "_etl/" + tableName
                        spark.sql("ALTER TABLE  " + tempDataBase + "." + tableName + "  ADD IF NOT EXISTS PARTITION (year=" +partitionlist[0] + ",month=" + partitionlist[1] + ",day=" + partitionlist[2] + ")")
                        spark.sql("ALTER TABLE  " + tempDataBase + "." + tableName + "  ADD IF NOT EXISTS PARTITION (year=" +partitionlist[0] + ",month=" + partitionlist[1] + ",day=" + partitionlist[3] + ")")
                        if tableName=="t_customer":
                            sql = "select * from  "+tempDataBase+"."+tableName
                            spark.sql(sql).withColumn('mobile', F.sha2(F.col('mobile'), 256)).drop('imei').drop('password').drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
                        elif tableName=="t_loan_app":
                            sql = "select * from  " + tempDataBase + "." + tableName
                            spark.sql(sql).drop('credential_no').drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
                        elif tableName=="t_personal_info":
                            sql = guestTable[tableName][0]+tempDataBase+"."+tableName
                            spark.sql(sql).drop('credential_no').drop('etldate').write.mode("overwrite").orc(guestPath)
                        elif tableName == "t_auto_review_loan":
                            sql = "select * from  " + tempDataBase + "." + tableName
                            spark.sql(sql).drop('name').drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
                        elif tableName == "t_lpay_deposit":
                            sql = "select *  from  " + tempDataBase + "." + tableName
                            spark.sql(sql).drop('name').drop("deposit_method").drop("deposit_channel").drop("out_deposit_no").drop("payment_code").drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
                        elif tableName == "t_thirdparty_data":
                            sql = guestTable[tableName][0] + tempDataBase + "." + tableName+guestTable[tableName][1]
                            spark.sql(sql).drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
                        elif tableName == "t_contact":
                            # 先给t_customer加上今天的partitions
                            spark.sql("ALTER TABLE  " + tempDataBase + ".t_customer  ADD IF NOT EXISTS PARTITION (year=" +partitionlist[0] + ",month=" + partitionlist[1] + ",day=" + partitionlist[2] + ")")
                            sql = guestTable[tableName][0]+tempDataBase+"."+tableName+guestTable[tableName][1]+tempDataBase+".t_customer "+guestTable[tableName][2]
                            spark.sql(sql).drop('etldate').write.mode("overwrite").orc(guestPath)
                        elif tableName == "t_record_personal_info":
                            sql = "select * from  " + tempDataBase + "." + tableName
                            spark.sql(sql).drop('credential_no').drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
						 elif tableName == "t_login_log":
                            sql = "select * from  " + tempDataBase + "." + tableName
                            spark.sql(sql).drop('mobile').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
                        else :
                            sql = "select * from  " + tempDataBase + "." + tableName
                            spark.sql(sql).drop('etldate').write.mode("overwrite").partitionBy("year","month","day").orc(guestPath)
