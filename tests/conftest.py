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

    # CHANGED: Added hive.metastore.catalog.default configuration to ensure
    # in-memory database isolation works flawlessly across views.
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("LoanBridge-UnitTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .getOrCreate()
        
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="session")
def loan_defaulters_schema():
    """
    Provides the Defaulters Schema as a reusable fixture.
    """
    return lds


# ===================================================================================
# CHANGED/ADDED: NEW FIXTURE FOR FEATURE/GOLD-LOAN-SCORE-BASELINE
# ===================================================================================
@pytest.fixture(scope="session")
def gold_app_conf():
    """
    Provides standard scoring matrix rules to test suites, matching the
    matrix layouts from configs/application.conf exactly.
    """
    return {
        "score.pts.unacceptable": "0",
        "score.pts.very_bad": "100",
        "score.pts.bad": "250",
        "score.pts.good": "500",
        "score.pts.very_good": "650",
        "score.pts.excellent": "800",
        "grade.cutoff.unacceptable": "750",
        "grade.cutoff.very_bad": "1000",
        "grade.cutoff.bad": "1500",
        "grade.cutoff.good": "2000",
        "grade.cutoff.very_good": "2500"
    }
# ===================================================================================