import pytest
import os
import sys
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a shared SparkSession for all tests.
    Uses the current Python executable to prevent version mismatches.
    """
    # Force Spark to use the exact Python binary running pytest
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("LoanBridge-UnitTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
        
    yield spark_session
    
    # Proper teardown
    spark_session.stop()