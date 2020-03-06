from __future__ import print_function

import sys

import numpy as np
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Python Demo")\
        .enableHiveSupport()\
        .getOrCreate()

    url = "jdbc:postgresql://lovina-prod.ct3lmpqmpvcq.ap-southeast-1.rds.amazonaws.com:5432/lovina"
    t_task = spark\
      .read\
      .format("jdbc")\
      .option("url", url)\
      .option("dbtable", "t_task")\
      .option("user", "lovina")\
      .option("password", "UFXXExqnXJix9b2eM9Ge")\
      .option("partitionColumn", "id")\
      .option("lowerBound", 100000)\
      .option("upperBound", 12783331)\
      .option("numPartitions", 1000)\
      .load()
    t_task.createOrReplaceTempView("t")
    bound = spark.sql("select max(id) max, min(id) min from t")
    max = bound.collect()[0].max
    min = bound.collect()[0].min
    t_task = spark\
      .read\
      .format("jdbc")\
      .option("url", url)\
      .option("dbtable", "t_task")\
      .option("user", "lovina")\
      .option("password", "UFXXExqnXJix9b2eM9Ge")\
      .option("partitionColumn", "id")\
      .option("lowerBound", min)\
      .option("upperBound", max)\
      .option("numPartitions", 1000)\
      .load()
    t_task.write.mode("overwrite").orc("s3://rupiahplus-data-warehouse/aliyun/lovina/t_task_1")

    spark.stop()
