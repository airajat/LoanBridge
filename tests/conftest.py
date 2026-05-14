import pytest
import os
import sys
from pyspark.sql import SparkSession
from lib.schemas import loan_defaulters_schema as lds

@pytest.fixture(scope="session")
def spark():
    """
    Creates a shared SparkSession for all tests.
    """
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
    spark_session.stop()

@pytest.fixture(scope="session")
def loan_defaulters_schema():
    """
    Provides the Defaulters Schema as a reusable fixture.
    """
    return lds