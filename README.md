#LoanBridge Data Engineering Pipeline

##Project Overview
This project is an industrial-grade PySpark ETL (Extract, Transform, Load) pipeline designed to process LendingClub loan and customer data. The pipeline ingests raw data from HDFS, applies rigorous cleaning and business transformation logic, and persists the results into a Silver Layer (Parquet) while segregating "Bad Data" for further audit.

##Tech Stack
Language: Python 3.11
Framework: PySpark (Spark 3.x)
Environment Management: Pipenv
Testing: Pytest
Cluster Manager: YARN (ITVersity Cluster)
Storage Format: Apache Parquet

##Key Features
Centralised Schema Management: Utilises StructType definitions in lib/schemas.py to enforce strict data typing across the entire pipeline.
Unified Error Handling: Both Loan and Customer datasets implement a "Reject Logic" framework. Records failing critical quality checks are saved to a dedicated error/ path with reason codes.
Complex Transformations:
Standardising loan terms (months to years).
Mapping and fallback logic for loan purposes.
Statistical imputation for missing employment lengths.
Robust Testing: A comprehensive unit test suite that simulates Spark environments locally using pytest.

Local Development & Testing
1. Setup Environment
Ensure you have Pipenv installed, then run:
pipenv install

2. Running Unit Tests
To run the transformation tests without version mismatch issues:
pipenv run pytest -v

Note: The tests/conftest.py is configured to automatically align the Spark worker version with your local Python executable.

Production Deployment (ITVersity Cluster)
Follow these steps to deploy and execute the pipeline on the YARN cluster.
1. Package Dependencies
Zip the library files and utility scripts for Spark distribution:

zip -r dependencies.zip lib/ logger.py

2. Deploy Artifact to Gateway
Upload the main script, dependencies, and configurations to the gateway node:
scp application_main.py dependencies.zip configs/*.conf <your_user>@g01.itversity.com:~/

3. Submit to YARN
Execute the job in client mode:

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

spark-submit \
    --master yarn \
    --deploy-mode client \
    --py-files dependencies.zip \
    --files application.conf,pyspark.conf \
    application_main.py DEV

Project Structure
Plaintext

LoanBridge/
├── application_main.py      # Entry point for the Spark job
├── lib/
│   ├── dataReader.py        # Logic for reading raw data (CSV/Parquet)
│   ├── dataManipulation.py  # Core transformation and cleaning logic
│   └── schemas.py           # Centralized StructType definitions
├── tests/
│   ├── conftest.py          # PySpark test fixtures and env config
│   └── test_transform.py    # Unit tests for transformations
├── configs/
│   └── application.conf     # Environment-specific paths and settings
├── dependencies.zip         # Packaged lib for cluster execution
└── README.md                # Project documentation

Data Quality Summary