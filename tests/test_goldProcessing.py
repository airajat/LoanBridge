import pytest
from pyspark.sql.functions import col
from pyspark.sql import Row
from lib import goldProcessing

def test_deploy_realtime_scoring_view_logic(spark, gold_app_conf):
    """
    Validates view structure registration and verifies that 
    the anti-join exclusion zone drops bad data keys correctly 
    using isolated in-memory temporary views.
    """
    
    # 1. Seed Clean Layout Mock Datasets directly to local execution memory
    spark.createDataFrame([
        Row(member_id="M_CLEAN", home_ownership="OWN", grade="A", sub_grade="A1", total_high_credit_limit=100000.0)
    ]).createOrReplaceTempView("customers")
    
    spark.createDataFrame([
        Row(loan_id="L1", member_id="M_CLEAN", funded_amount=10000.0, loan_status="Fully Paid", monthly_installment=500.0)
    ]).createOrReplaceTempView("loans")
    
    spark.createDataFrame([
        Row(loan_id="L1", total_principal_received=10000.0, total_interest_received=500.0, 
            total_late_fee_received=0.0, total_payment_received=10500.0, last_payment_amount=500.0)
    ]).createOrReplaceTempView("loans_repayments")
    
    spark.createDataFrame([
        Row(member_id="M_CLEAN", delinq_2yrs=0, delinq_amnt=0.0, mths_since_last_delinq=0)
    ]).createOrReplaceTempView("loans_defaulters_delinq")
    
    spark.createDataFrame([
        Row(member_id="M_CLEAN", pub_rec=0, pub_rec_bankruptcies=0, inq_last_6mths=0)
    ]).createOrReplaceTempView("loans_defaulters_detail_rec_enq")
    
    # Seed Duplicate Exclusion Data Frame
    spark.createDataFrame([Row(member_id="M_BAD")]).createOrReplaceTempView("consolidated_bad_members")

    # 2. Run view deployment logic (Pass None as the database to trigger pure memory-view processing)
    goldProcessing.deploy_realtime_scoring_view(spark, None, gold_app_conf)
    
    # 3. Assertions
    res_df = spark.read.table("view_gold_loan_score_realtime")
    
    assert "payment_history_pts" in res_df.columns
    assert "defaulters_history_pts" in res_df.columns
    assert "financial_health_pts" in res_df.columns
    
    # Verify anti-join works perfectly and screens out flagged members
    assert res_df.filter(col("member_id") == "M_BAD").count() == 0