# from __future__ import print_function

import sys

import numpy as np
from pyspark.sql import SparkSession
from datetime import datetime,timedelta
import pyspark.sql.functions as F

import pytz
# print(datetime.now(pytz.timezone("Asia/Shanghai")).strftime( '%Y-%m-%d'))
beforeyesterday=(datetime.now()+ timedelta(-1)).strftime('%Y-%m-%d')+" "+(datetime.now()-timedelta(minutes=60)).strftime('%H:00:00')
yesterday=(datetime.now()).strftime('%Y-%m-%d')
partitionlist=yesterday.split("-",3)
yesterday=yesterday+" 23:59:59.999"


# yesterday=(datetime.now()+ timedelta(-1)).strftime( '%Y-%m-%d')
# beforeyesterday=(datetime.now()+ timedelta(-2)).strftime( '%Y-%m-%d')
dbmap={"banda_stream_etl":"1"} 

# "luzon_stream_etl":"1" ,

partitionstr=" year(to_date(etldate)) as year,if(length(MONTH(to_date(etldate)))=1,concat('0',MONTH(to_date(etldate))),MONTH(to_date(etldate))) as month,if(length(day(to_date(etldate)))=1,concat('0',day(to_date(etldate))),day(to_date(etldate))) as day"

table_head="etldate,etlindex";
histable_head="etldate,0 as etlindex";


nowsql_1="select  *  from   " ;
nowsql_2=" where  date_format(etldate,'yyyy-MM-dd HH:mm:ss.SSS')  > timestamp('"+beforeyesterday+"')" +" and  date_format(etldate,'yyyy-MM-dd HH:mm:ss.SSS')  < timestamp('"+yesterday+"') order by id desc ";



tablesql_1=" FROM  (   SELECT    *  , row_number() OVER (PARTITION BY id  ORDER BY CAST(etldate AS timestamp) DESC ,if(etlindex is null ,0,etlindex)  desc)  row_num FROM  "
tablesql_2=" ORDER BY id  ASC )    WHERE row_num = 1 AND kind <> 'delete' "

def getTableColum(b):
    colum=""
    for index in range(len(b)):
        if(index>2 and index<len(b)-8):   
            colum=colum+b[index]["col_name"]+", "
    return colum
# print(__name__)builtins
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Python Demo")\
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.driver.maxResultSize", "4g") \
        .enableHiveSupport()\
        .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict");
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    databases=spark.sql("show databases")
    databases=databases.collect()
    for row in databases:
        databaseName=row["databaseName"]
        if databaseName != "" and  databaseName in dbmap :
            print(databaseName)
            databasesql="show tables in "+databaseName
            tables=spark.sql(databasesql)
            tablelist=tables.collect();
            for row in tablelist:
                tableName=row["tableName"]
                sql="desc " +databaseName+"."+tableName;
                tableSchema= spark.sql(sql).collect()
                colum=getTableColum(tableSchema)
                tablepath="s3://rupiahplus-data-warehouse/stream/"+databaseName+"daily/"+row["tableName"]
                hissql="select etldate,0 as etlindex,  "+colum+" year,month,day"+" ,'insert' as kind from "+ databaseName+"_daily"+"."+tableName
                sql="select etldate,etlindex,  "+colum+" year,month,day"+" ,kind from "+ databaseName+"."+tableName+nowsql_2
                nowdata=spark.sql(sql)
                spark.catalog.refreshTable(databaseName+"_daily"+"."+tableName)
                hisdata =spark.sql(hissql)
                hisdata.union(nowdata).createOrReplaceTempView("tmp");
                sql1="select etldate,etlindex," +colum+partitionstr+ tablesql_1 +" tmp "+ tablesql_2 
                newdata =spark.sql(sql1).drop("etlindex")
                newdata.write.mode("overwrite").orc("hdfs:///table_s3_etl_tmp/")
                jsonDf = spark.read.format("orc").load( "hdfs:///table_s3_etl_tmp/")
                jsonDf.write.mode("overwrite").partitionBy("year","month","day").orc(tablepath)
                #add   partitions
                partitionsql="ALTER TABLE "+databaseName+"_daily"+"."+tableName+"  ADD IF NOT EXISTS PARTITION (year=" +partitionlist[0] + ",month=" + partitionlist[1] + ",day=" + partitionlist[2] + ")"
                spark.sql(partitionsql)
                spark.catalog.dropTempView("tmp")

