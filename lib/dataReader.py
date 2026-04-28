from lib.configReader import get_app_config

def read_customers(spark, env):
    conf = get_app_config(env)
    customer_file_path = conf["customer.obj.path"]
    
    customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(customer_schema) \
        .load(customer_file_path)