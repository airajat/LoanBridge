from pyspark.sql import SparkSession
from pyspark import SparkConf
from lib import configReader
import getpass
import os

def get_spark_session(env):
    username = getpass.getuser()
    conf_dict = configReader.get_pyspark_config(env)
    
    spark_conf = SparkConf()
    for key, val in conf_dict.items():
        spark_conf.set(key, val)
    
    # Environment-specific dynamic overrides
    if env != "LOCAL":
        spark_conf.set("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
        spark_conf.set("spark.ui.port", "0")
    else:
        spark_conf.set("spark.driver.bindAddress", "127.0.0.1")
        # INDUSTRIAL FIX: Force Spark to use our local log4j2.properties file
        # This tells the JVM where to look for the logging contract
        spark_conf.set("spark.driver.extraJavaOptions", "-Dlog4j2.configurationFile=log4j2.properties")

    return SparkSession.builder \
        .config(conf=spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()