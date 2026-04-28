from pyspark.sql import SparkSession
import getpass

def get_spark_session(env):
    username = getpass.getuser()

    if env == "LOCAL":
        master = "local[*]"
    else:
        # Cluster mode
        master = "yarn"

    return SparkSession.builder \
        .appName(f"LoanBridge_{username}") \
        .master(master) \
        .config('spark.ui.port', '0') \
        .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
        .config('spark.shuffle.useOldFetchProtocol', 'true') \
        .enableHiveSupport() \
        .getOrCreate()