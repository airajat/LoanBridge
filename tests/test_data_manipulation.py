import pytest
from lib import dataManipulation
from lib.schemas import loan_clean_schema, customer_schema

@pytest.mark.transformation
def test_emp_length_imputation(spark):
    test_data = [
        ("1", "Manager", "10 years", "RENT", 50000.0, "NY", "12345", "USA", "A", "A1", "V", 100000.0, "IND", 0.0, None),
        ("2", "Manager", "10 years", "RENT", 60000.0, "CA", "12345", "USA", "A", "A1", "V", 100000.0, "IND", 0.0, None),
        ("3", "Manager", None, "RENT", 70000.0, "TX", "12345", "USA", "A", "A1", "V", 100000.0, "IND", 0.0, None)
    ]
    df = spark.createDataFrame(test_data, schema=customer_schema)
    
    good_df, bad_df = dataManipulation.clean_customer_data(df)
    results = good_df.collect()
    
    # Asserting that the mean (10) was filled into the 3rd row
    assert results[2]["emp_length"] == 10
    assert bad_df.count() == 0

@pytest.mark.transformation
def test_loan_term_conversion(spark):
    test_data = [
        ("505", "1", 1000.0, 1000.0, "36 months", 0.1, 100.0, "2024-01-01", "Fully Paid", "debt_consolidation")
    ]
    schema = ["loan_id", "member_id", "loan_amount", "funded_amount", "loan_term_months", 
              "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    
    df = spark.createDataFrame(test_data, schema)
    good_loans, _ = dataManipulation.clean_loans_data(df)
    
    result = good_loans.collect()[0]
    assert result["loan_term_years"] == 3

@pytest.mark.transformation
def test_loan_rejects_logic(spark):
    test_data = [
        ("1", "1", 1000.0, 1000.0, "36 months", 0.1, 100.0, "2024-01-01", "Paid", "debt"),
        ("2", "2", None, 1000.0, "36 months", 0.1, 100.0, "2024-01-01", "Paid", "debt")
    ]
    schema = ["loan_id", "member_id", "loan_amount", "funded_amount", "loan_term_months", 
              "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    
    df = spark.createDataFrame(test_data, schema)
    good_df, bad_df = dataManipulation.clean_loans_data(df)
    
    assert good_df.count() == 1
    assert bad_df.count() == 1
    assert bad_df.collect()[0]["reject_reason"] == "Null values in critical columns"

@pytest.mark.parametrize("raw_purpose, expected_purpose", [
    ("debt_consolidation", "debt_consolidation"),
    ("credit_card", "credit_card"),
    ("buying_a_spaceship", "other"),
    (None, None)
])
def test_loan_purpose_logic(spark, raw_purpose, expected_purpose):
    # We use the clean schema to ensure Spark knows the types for 'None' values
    test_data = [("101", "1", 1000.0, 1000.0, "36 months", 0.1, 100.0, "2024-01-01", "Paid", raw_purpose, "Some Title")]
    df = spark.createDataFrame(test_data, schema=loan_clean_schema)

    success_df, error_df = dataManipulation.clean_loans_data(df)

    if raw_purpose is None:
        assert error_df.count() == 1
    else:
        assert success_df.collect()[0]["loan_purpose"] == expected_purpose