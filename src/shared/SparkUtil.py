# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession


class SparkUtil(object):

    @staticmethod
    def spark_session(app_name, master):
        spark_session = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()

        return spark_session

    @staticmethod
    def spark_session(app_name):
        spark_session = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

        return spark_session

    @staticmethod
    def repartition_size():
        default_repartition_size = 512

        # sc = spark_session.sparkContext
        # cores = sc.getConf().get("spark.executor.cores")
        # instances = sc.getConf().get("spark.executor.instances")
        # name = sc.getConf().get("spark.app.name")
        # print(str(name) + " " + str(cores))
        return default_repartition_size


if __name__ == '__main__':
    spark = SparkUtil.spark_session("test")
    SparkUtil.repartition_size(spark)
