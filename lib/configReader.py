import configparser
import os

def get_app_config(env):
    config = configparser.ConfigParser()
    if os.path.exists("application.conf"):
        config.read("application.conf")
    elif os.path.exists("configs/application.conf"):
        config.read("configs/application.conf")
    
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf

def get_pyspark_config(env):
    config = configparser.ConfigParser()
    if os.path.exists("pyspark.conf"):
        config.read("pyspark.conf")
    elif os.path.exists("configs/pyspark.conf"):
        config.read("configs/pyspark.conf")
    
    # Returning a dictionary is cleaner for the utils.py loop
    pyspark_conf = {}
    for (key, val) in config.items(env):
        pyspark_conf[key] = val
    return pyspark_conf