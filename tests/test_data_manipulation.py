import pytest
from lib import dataManipulation, dataReader, utils

@pytest.mark.transformation
def test_emp_length_imputation(spark):
    # Add a dummy 'addr_state' value so the function doesn't crash
    test_data = [
        ("1", "10 years", 50000.0, "NY"), 
        ("2", "10 years", 60000.0, "CA"),
        ("3", None, 70000.0, "TX")
    ]
    # This schema must contain every column clean_customer_data() touches
    schema = ["member_id", "emp_length", "annual_inc", "addr_state"]
    df = spark.createDataFrame(test_data, schema)
    
    output_df = dataManipulation.clean_customer_data(df)
    results = output_df.collect()
    
    assert results[2]["emp_length"] == 10

@pytest.mark.parametrize("raw_state, expected_outcome", [
    ("California", "NA"),  # Too long
    ("NY", "NY"),          # Perfect
    ("A", "A"),            # Short but valid per your logic (< 2)
    ("United States", "NA") # Way too long
])
def test_state_logic(spark, raw_state, expected_outcome):
    # 1. Arrange (Minimal data)
    test_data = [("1", "5 years", 50000.0, raw_state)]
    schema = ["member_id", "emp_length", "annual_inc", "addr_state"]
    df = spark.createDataFrame(test_data, schema)
    
    # 2. Act
    output_df = dataManipulation.clean_customer_data(df)
    actual_state = output_df.collect()[0]["address_state"]
    
    # 3. Assert
    assert actual_state == expected_outcome

