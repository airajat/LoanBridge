from pyspark.sql import SparkSession
from pyspark import SparkConf
from lib import ConfigReader
import getpass

def get_spark_session(env):
    username = getpass.getuser()
    conf_dict = ConfigReader.get_pyspark_config(env)
    
    spark_conf = SparkConf()
    for key, val in conf_dict.items():
        spark_conf.set(key, val)
    
    # Environment-specific dynamic overrides
    if env != "LOCAL":
        spark_conf.set("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
        spark_conf.set("spark.ui.port", "0")
    else:
        spark_conf.set("spark.driver.bindAddress", "127.0.0.1")

    return SparkSession.builder \
        .config(conf=spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()