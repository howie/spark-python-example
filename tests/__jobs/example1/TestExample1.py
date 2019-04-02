import random
import unittest

from shared.SparkUtil import SparkUtil


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


        :return:
        """



