LoanBridge Data Engineering Pipeline

Project Overview:

LoanBridge is an industrial-grade PySpark ETL pipeline designed to ingest, clean, and transform LendingClub financial data. 
The system implements a Medallion Architecture, migrating raw CSV data (Bronze) into a strictly typed, optimized Parquet format (Silver).

Data Architecture & Lineage:

The pipeline follows a structured path to ensure data integrity and auditability:

    Bronze Layer: Raw CSV ingestion from HDFS with enforced schemas.

    Transformation Engine: Business logic application, null handling, and data sanitization.
    
    Silver Layer: Cleaned data persisted as Snappy-compressed Parquet for downstream analytics.
    
    Error Zone: Quality-checked "Bad Data" segregated for reconciliation and debugging.

TechStack

Engine: PySpark (Spark 3.x) on YARN
Language: Python 3.11
Env Management: Pipenv (Virtual Environments)
Testing: Pytest (Automated Unit Testing)
Storage: HDFS & Apache Parquet

Key Features

Strict Schema Enforcement: Centralized StructType definitions in lib/schemas.py eliminate data type drift.
Unified Reject Framework: Consistent logic across all datasets captures and logs rejected records with specific reason codes.
Complex Business Logic:
    Customers: Statistical imputation of employment length and address cleaning.
    Loans: Normalization of loan terms and categorization of loan purposes.
    Repayments: Dynamic recalculation of total_payment_received and sanitization of legacy "0.0" date strings.
    
Automated Reconciliation: Every run generates a "Data Balance Sheet" in the logs, comparing Input vs. Output vs. Rejects.


Execution Guide
1. Local Testing    
    pipenv install
    pipenv run pytest -v
2. Cluster Deployment (YARN)
    Package the library
    zip -r dependencies.zip lib/ logger.py

# Deploy to Gateway
scp application_main.py dependencies.zip configs/*.conf <user>@g01.itversity.com:~/

# Submit Job (DEV mode)
spark-submit \
    --master yarn \
    --deploy-mode client \
    --py-files dependencies.zip \
    --files application.conf,pyspark.conf \
    application_main.py DEV

Project StructurePlaintextLoanBridge/
├── application_main.py      # Job Orchestrator
├── lib/
│   ├── dataReader.py        # Bronze Ingestion Logic
│   ├── dataManipulation.py  # Silver Transformation Logic
│   └── schemas.py           # Centralized Source of Truth for Data Types
├── tests/                   # Pytest Suite
├── configs/                 # Environment-specific (LOCAL/DEV/PROD)
└── README.md                # Technical Documentation


Data Quality Summary (Silver Layer)
Dataset      IngestionFormat  OutputFormat         Success Path (Silver)   Reject Path (Error)
Customers         CSV        Parquet              /.../silver/customers         /.../error/customersLoans    CSV        Parquet              /.../silver/loans             /.../error/loansRepayments   CSV        Parquet              /.../silver/loanrepayments    /.../error/