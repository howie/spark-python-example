import unittest

from pyspark.sql.functions import collect_list, col, from_json, get_json_object, when, explode
from pyspark.sql.types import *

from src.shared.SparkUtil import SparkUtil


class TestExample(unittest.TestCase):

    def setUp(self):
        """
        Start Spark, define config and path to test data
        """
        self.spark = SparkUtil.spark_session("test")

    def tearDown(self):
        """
        Stop Spark
        """
        self.spark.stop()

    def test_run_example(self):
        """
id_1| id_2| id_3|timestamp|thing1|thing2|thing3
A   | b   | c   |time_0   |1.2   |1.3    |2.5
A   | b   | c   |time_1   |1.1   |1.5    |3.4
A   | b   | c   |time_2   |2.2   |2.6    |2.9
A   | b   | d   |time_0   |5.1   |5.5    |5.7
A   | b   | d   |time_1   |6.1   |6.2    |6.3
A   | b   | e   |time_0   |0.1   |0.5    |0.9
A   | b   | e   |time_1   |0.2   |0.3    |0.6

        :return:
        """
        exampleDf = self.spark.createDataFrame(
            [('A', 'b', 'c', 'time_0', 1.2, 1.3, 2.5),
             ('A', 'b', 'c', 'time_1', 1.1, 1.5, 3.4),
             ],
            ("id_1", "id_2", "id_3", "timestamp", "thing1", "thing2", "thing3"))

        exampleDf.show()

        ans = exampleDf.groupBy(col("id_1"), col("id_2"), col("id_3")) \
            .agg(collect_list(col("timestamp")),
                 collect_list(col("thing1")),
                 collect_list(col("thing2")))

        ans.show()

    def test_run_example3(self):
        df = self.spark.createDataFrame(['[{"key": "value1"}, {"key": "value2"}]'], StringType())
        df.show(1, False)
        schema = ArrayType(StructType([StructField("key", StringType(), True)]))

        df = df.withColumn("json", from_json("value", schema))
        df.show()

    def test_run_example2(self):
        df = self.spark.createDataFrame(['{"a":1}', '{"a":1, "b":2}', '{"a":1, "b":2, "c":3}'], StringType())

        df.show(3, False)

        df = df.withColumn("a", get_json_object("value", '$.a')) \
            .withColumn("b",
                        when(get_json_object("value", '$.b').isNotNull(), get_json_object("value", '$.b')).otherwise(0)) \
            .withColumn("c",
                        when(get_json_object("value", '$.c').isNotNull(), get_json_object("value", '$.c')).otherwise(0))

        df.show(3, False)
        df.printSchema()

    def test_run_example4(self):
        """
        https://stackoverflow.com/questions/55947954/
        :return:
        """
        df = self.spark.createDataFrame(
            [('001', '[{"index": 1}, {"index": 2}]'),
             ('002', '[{"index": 3}, {"index": 4}]'),
             ],
            ("id", "data"))

        schema = ArrayType(StructType([StructField("index", IntegerType())]))
        df = df.withColumn("json", from_json("data", schema))
        df.printSchema()
        df.show(100)
        df = df.select(col("id"), explode("json").alias("index")) \
            .select(col("id"), col("index").index)

        df.printSchema()
        df.show(100)
