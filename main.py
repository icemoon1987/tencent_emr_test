#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import time
import logging
import json
import re
from datetime import datetime, timedelta
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add

def get_info(inline):
    try:
        indata = json.loads(inline)
        model = indata["model"]
    except:
        return []

    return [(model, 1)]

def main():
    spark = SparkSession.builder.appName("example_data_panwenhai").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")

    #in_path = "file:///home/hadoop/emr_test/test_data"
    #in_path = "cosn://s3-data-test-1301030913/mxplayer_strategy_statistic/intermediate/20200101/mxplayer_strategy_statistic/intermediate/20200101"
    in_path = "cosn://s3-data-test-1301030913/test_data"
    out_path = "cosn://s3-data-test-1301030913/panwenhai/output"

    lines = spark.sparkContext.textFile(in_path).coalesce(1000,False)

    result = lines.flatMap(lambda line: get_info(line))

    result = result.reduceByKey(add).map(lambda item: "%s,%s" % (item[0], item[1]))

    result.saveAsTextFile(out_path)

    #for item in result.take(10):
        #print item

    return


if __name__ == "__main__":
    main()
