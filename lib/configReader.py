import configparser
from pyspark import SparkConf

def get_app_config(env):
    """
    Reads application.conf and returns a dictionary of settings 
    for the specific environment.
    """
    config = configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf

def get_pyspark_config(env):
    """
    Reads pyspark.conf and returns a SparkConf object 
    to be used during SparkSession creation.
    """
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf