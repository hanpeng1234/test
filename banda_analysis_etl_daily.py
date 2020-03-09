# from __future__ import print_function

import sys

import numpy as np
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as F

import pytz

# print(datetime.now(pytz.timezone("Asia/Shanghai")).strftime( '%Y-%m-%d'))
#北京时间0点半执行，相当于utc时间昨天的16:30 ，所以时间不-1天
# yesterday = datetime.now().strftime('%Y-%m-%d')
yesterday = datetime.now().strftime('%Y-%m-%d')
partitionlist=yesterday.split("-",3)
beforeyesterday = (datetime.now() + timedelta(-1)).strftime('%Y-%m-%d')
#防止当天出错之后,八点之后运行会丢失分区
partitionlist.append(beforeyesterday.split("-",3)[2])
dbmap = {"banda_guest": "1"}
#是否特殊special，特殊的需要自己写sql
# print(__name__)builtins
# __main__
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
    tablelist=["`banda-etl-s3`.`t_customer`","`banda-etl-s3`.`t_customer_install_info`","`banda-etl-s3`.`t_whitelist_old_customer`","`banda-etl-s3`.`t_loan_app`","`banda-etl-s3`.`t_loan_app_status_log`","`banda-etl-s3`.`t_lpay`",
               "`banda-etl-s3`.`t_record_personal_info`","`banda-etl-s3`.`t_record_employment`","`banda-etl-s3`.`t_record_contact`","`banda-etl-s3`.`t_record_file`"];
#    partitionsql_1=   "ALTER TABLE  #table# ADD IF NOT EXISTS PARTITION (year=" +partitionlist[0] + ",month=" + partitionlist[1] + ",day=" + partitionlist[2] + ");"
#    partitionsql_2=   "ALTER TABLE  #table# ADD IF NOT EXISTS PARTITION (year=" +partitionlist[0] + ",month=" + partitionlist[1] + ",day=" + partitionlist[3] + ");"
#     for  row in tablelist:
#         #添加分区 
# #       spark.sql(partitionsql)
#         partitionsql_1.replace("#table#", row) 
#         spark.sql(partitionsql_1)
#         partitionsql_2.replace("#table#", row) 
#         spark.sql(partitionsql_2)
    df1=spark.sql("""
       select cus.id as customer_id,cus.create_time as register_time,cus.last_login_time,cus.inviter_id,
        case when whitelist.customer_id > 0 then 'Y' else 'N' end as is_whitelist,
        install.af_channel,install.media_source,install.campaign,install.agency,install.fb_campaign_id,install.install_time,
        case when record.id >0 then 'Y' else 'N' end as is_record,record.record_time,
        loan.first_apply_time,loan.first_issue_time,loan.first_repayday_create_time,loan.total_issue_time,
        loan.total_issue_amount,loan.total_paid_amount,loan.last_loan_status,loan.last_loan_due_date,
        loan.current_loan_out,
        loan.current_loan_principal
        from `banda-etl-s3`.`t_customer` cus
        left join `banda-etl-s3`.`t_customer_install_info` install
        on cus.id = install.customer_id
        left join `banda-etl-s3`.`t_whitelist_old_customer` whitelist
        on cus.id = whitelist.customer_id
        left join
        (
          select loan_detail.*,app.status as last_loan_status, 
          case when app.status in ('CURRENT','OVERDUE','PAID_OFF','GRACE_PERIOD') then pay.due_date else null end as last_loan_due_date,
          case when app.status in ('CURRENT','OVERDUE','GRACE_PERIOD') 
          then coalesce (pay.principal_accr ,0)+ coalesce (pay.interest_accr,0) + coalesce (pay.default_accr,0) + coalesce (pay.service_fee_accr,0)+coalesce (pay.default_fee_accr,0)-(coalesce (pay.principal_paid ,0)+ coalesce (pay.interest_paid,0) + coalesce (pay.default_paid,0) + coalesce (pay.service_fee_paid,0)+coalesce (pay.default_fee_paid,0)) else null end as current_loan_out,
          case when app.status in ('CURRENT','OVERDUE','GRACE_PERIOD') then coalesce(pay.principal_accr,0)-coalesce(pay.principal_paid,0) else null end as current_loan_principal
          from
          (
          select app.customer_id,min(app.create_time) as first_apply_time,min(cu_status.create_time) as first_issue_time,
          min(if(app.loan_type = 'RE_PAYDAY',app.create_time,null)) as  first_repayday_create_time,
          count(if(cu_status.id>0 and app.loan_type != 'ROLLOVER',app.id,null)) as total_issue_time,
          sum(if(cu_status.id>0 and app.loan_type != 'ROLLOVER',app.amount,0)) as total_issue_amount,
          sum(if(cu_status.id>0 ,coalesce (pay.principal_paid ,0)+ coalesce (pay.interest_paid,0) + coalesce (pay.default_paid,0) + coalesce (pay.rollover_fee_paid,0)+ coalesce (pay.service_fee_paid,0)-coalesce (pay.erase_amount,0)-coalesce (pay.coupon_amount,0),0)) as total_paid_amount,max(app.id) as last_loan_id

          from `banda-etl-s3`.`t_loan_app` app
          left join `banda-etl-s3`.`t_loan_app_status_log` cu_status
          on app.id = cu_status.loan_app_id and cu_status.new_status = 'CURRENT'
          left join `banda-etl-s3`.`t_lpay` pay
          on app.id = pay.loan_app_id
          where app.create_time +interval '7'hour  > date '2019-08-01'
          group by app.customer_id 
          )loan_detail
          left join `banda-etl-s3`.`t_loan_app` app on loan_detail.last_loan_id=app.id
          left join `banda-etl-s3`.`t_lpay` pay on loan_detail.last_loan_id = pay.loan_app_id

        )loan 
        on cus.id = loan.customer_id
        left join 
        (
          select id,
           case when pi_time > emp_time and pi_time > contact_time and pi_time > file_time then pi_time 
                when emp_time > pi_time and emp_time > contact_time and emp_time > file_time then emp_time 
                when contact_time > pi_time and contact_time > emp_time and contact_time > file_time then contact_time
                else file_time end as record_time
          from
          (
              select customer.id, 
              min(pi.create_time) as pi_time,min(emp.create_time) as emp_time,
              min(contact.create_time) as contact_time,min(file.create_time) as file_time
              from `banda-etl-s3`.`t_customer` customer
              LEFT JOIN  `banda-etl-s3`.`t_record_personal_info` pi
                  ON pi.customer_id = customer.id and pi.gender !=''
              LEFT JOIN  `banda-etl-s3`.`t_record_employment` emp
                  ON emp.customer_id = customer.id
              LEFT JOIN  `banda-etl-s3`.`t_record_contact` contact
                  ON contact.customer_id = customer.id
              LEFT JOIN  `banda-etl-s3`.`t_record_file` file
                  ON file.customer_id = customer.id
                      AND file.file_type = 'KTP_PHOTO'
              where date(customer.create_time + interval '7' hour) > date '2019-08-01' 
              and pi.customer_id >0 and  emp.customer_id >0 and contact.customer_id>0 and file.customer_id >0
              group by customer.id
          )record_detail
        )record
        on cus.id = record.id
        order by cus.id desc
    """)
    df1.write.mode("overwrite").orc("s3://rupiahplus-data-warehouse/etl/banda/analysis/t_customer_detail")
