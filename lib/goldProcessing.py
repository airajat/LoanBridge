import logging
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def deploy_realtime_scoring_view(spark, database, app_conf):
    """
    Binds system configuration rules directly to the active session metadata 
    and provisions the scoring view topology. Works seamlessly in prod and testing.
    """
    logger.info("Injecting rule configurations into Hive context...")
    spark.conf.set("spark.sql.unacceptable_rated_pts", app_conf["score.pts.unacceptable"])
    spark.conf.set("spark.sql.very_bad_rated_pts", app_conf["score.pts.very_bad"])
    spark.conf.set("spark.sql.bad_rated_pts", app_conf["score.pts.bad"])
    spark.conf.set("spark.sql.good_rated_pts", app_conf["score.pts.good"])
    spark.conf.set("spark.sql.very_good_rated_pts", app_conf["score.pts.very_good"])
    spark.conf.set("spark.sql.excellent_rated_pts", app_conf["score.pts.excellent"])

    spark.conf.set("spark.sql.unacceptable_grade_pts", app_conf["grade.cutoff.unacceptable"])
    spark.conf.set("spark.sql.very_bad_grade_pts", app_conf["grade.cutoff.very_bad"])
    spark.conf.set("spark.sql.bad_grade_pts", app_conf["grade.cutoff.bad"])
    spark.conf.set("spark.sql.good_grade_pts", app_conf["grade.cutoff.good"])
    spark.conf.set("spark.sql.very_good_grade_pts", app_conf["grade.cutoff.very_good"])

    db_prefix = f"{database}." if database else ""
    view_type = "VIEW" if database else "TEMPORARY VIEW"
    bad_members_table = "consolidated_bad_members" if not database else f"{database}.view_internal_bad_members"

    logger.info("Deploying payment history metrics view...")
    ph_query = f"""
    CREATE OR REPLACE {view_type} {db_prefix}view_staging_ph_pts AS
    SELECT 
        l.member_id,
        CASE 
            WHEN r.last_payment_amount < (l.monthly_installment * 0.5) THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
            WHEN r.last_payment_amount >= (l.monthly_installment * 0.5) AND r.last_payment_amount < l.monthly_installment THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
            WHEN r.last_payment_amount = l.monthly_installment THEN ${{hiveconf:spark.sql.good_rated_pts}}
            WHEN r.last_payment_amount > l.monthly_installment AND r.last_payment_amount <= (l.monthly_installment * 1.50) THEN ${{hiveconf:spark.sql.very_good_rated_pts}}
            WHEN r.last_payment_amount > (l.monthly_installment * 1.50) THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
            ELSE ${{hiveconf:spark.sql.unacceptable_rated_pts}}
        END AS last_payment_pts,
        CASE 
            WHEN r.total_payment_received >= (l.funded_amount * 0.50) THEN ${{hiveconf:spark.sql.very_good_rated_pts}}
            WHEN r.total_payment_received < (l.funded_amount * 0.50) AND r.total_payment_received > 0 THEN ${{hiveconf:spark.sql.good_rated_pts}}
            WHEN r.total_payment_received = 0 OR r.total_payment_received IS NULL THEN ${{hiveconf:spark.sql.unacceptable_rated_pts}}
        END AS total_payment_pts
    FROM {db_prefix}loans_repayments r
    INNER JOIN {db_prefix}loans l ON l.loan_id = r.loan_id 
    WHERE l.member_id NOT IN (SELECT member_id FROM {bad_members_table})
    """
    spark.sql(ph_query)

    logger.info("Deploying delinquency and analysis ledger view...")
    ldh_query = f"""
    CREATE OR REPLACE {view_type} {db_prefix}view_staging_ldh_ph_pts AS
    SELECT 
        p.*,
        CASE 
            WHEN d.delinq_2yrs = 0 THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
            WHEN d.delinq_2yrs BETWEEN 1 AND 2 THEN ${{hiveconf:spark.sql.bad_rated_pts}}
            WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
            WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL THEN ${{hiveconf:spark.sql.unacceptable_grade_pts}}
        END AS delinq_pts,
        CASE 
            WHEN e.pub_rec = 0 THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
            WHEN e.pub_rec BETWEEN 1 AND 2 THEN ${{hiveconf:spark.sql.bad_rated_pts}}
            WHEN e.pub_rec BETWEEN 3 AND 5 THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
            WHEN e.pub_rec > 5 OR e.pub_rec IS NULL THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
        END AS public_records_pts,
        CASE 
            WHEN e.pub_rec_bankruptcies = 0 THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
            WHEN e.pub_rec_bankruptcies BETWEEN 1 AND 2 THEN ${{hiveconf:spark.sql.bad_rated_pts}}
            WHEN e.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
            WHEN e.pub_rec_bankruptcies > 5 OR e.pub_rec_bankruptcies IS NULL THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
        END AS public_bankruptcies_pts,
        CASE 
            WHEN e.inq_last_6mths = 0 THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
            WHEN e.inq_last_6mths BETWEEN 1 AND 2 THEN ${{hiveconf:spark.sql.bad_rated_pts}}
            WHEN e.inq_last_6mths BETWEEN 3 AND 5 THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
            WHEN e.inq_last_6mths > 5 OR e.inq_last_6mths IS NULL THEN ${{hiveconf:spark.sql.unacceptable_rated_pts}}
        END AS enq_pts
    FROM {db_prefix}loans_defaulters_detail_rec_enq e
    INNER JOIN {db_prefix}loans_defaulters_delinq d ON d.member_id = e.member_id
    INNER JOIN {db_prefix}view_staging_ph_pts p ON p.member_id = e.member_id 
    WHERE e.member_id NOT IN (SELECT member_id FROM {bad_members_table})
    """
    spark.sql(ldh_query)

    logger.info("Compiling unified gold dynamic risk scoring fabric view...")
    fh_view_query = f"""
    CREATE OR REPLACE {view_type} {db_prefix}view_gold_loan_score_realtime AS
    SELECT 
        ldef.member_id,
        ((last_payment_pts + total_payment_pts) * 0.20) AS payment_history_pts,
        ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) AS defaulters_history_pts,
        ((
            CASE 
                WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
                WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${{hiveconf:spark.sql.good_rated_pts}}
                WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${{hiveconf:spark.sql.bad_rated_pts}}
                WHEN LOWER(l.loan_status) LIKE '%late%' THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
                WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${{hiveconf:spark.sql.unacceptable_rated_pts}}
                ELSE ${{hiveconf:spark.sql.unacceptable_rated_pts}}
            END +
            CASE 
                WHEN LOWER(c.home_ownership) LIKE '%own' THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
                WHEN LOWER(c.home_ownership) LIKE '%rent' THEN ${{hiveconf:spark.sql.good_rated_pts}}
                WHEN LOWER(c.home_ownership) LIKE '%mortgage' THEN ${{hiveconf:spark.sql.bad_rated_pts}}
                ELSE ${{hiveconf:spark.sql.very_bad_rated_pts}}
            END +
            CASE 
                WHEN l.funded_amount <= (c.total_high_credit_limit * 0.10) THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
                WHEN l.funded_amount > (c.total_high_credit_limit * 0.10) AND l.funded_amount <= (c.total_high_credit_limit * 0.20) THEN ${{hiveconf:spark.sql.very_good_rated_pts}}
                WHEN l.funded_amount > (c.total_high_credit_limit * 0.10) AND l.funded_amount <= (c.total_high_credit_limit * 0.20) THEN ${{hiveconf:spark.sql.very_good_rated_pts}}
                WHEN l.funded_amount > (c.total_high_credit_limit * 0.20) AND l.funded_amount <= (c.total_high_credit_limit * 0.30) THEN ${{hiveconf:spark.sql.good_rated_pts}}
                WHEN l.funded_amount > (c.total_high_credit_limit * 0.30) AND l.funded_amount <= (c.total_high_credit_limit * 0.50) THEN ${{hiveconf:spark.sql.bad_rated_pts}}
                WHEN l.funded_amount > (c.total_high_credit_limit * 0.50) AND l.funded_amount <= (c.total_high_credit_limit * 0.70) THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
                ELSE ${{hiveconf:spark.sql.unacceptable_rated_pts}}
            END +
            CASE 
                WHEN c.grade = 'A' AND c.sub_grade = 'A1' THEN ${{hiveconf:spark.sql.excellent_rated_pts}}
                WHEN c.grade = 'A' AND c.sub_grade = 'A2' THEN (${{hiveconf:spark.sql.excellent_rated_pts}} * 0.95)
                WHEN c.grade = 'A' AND c.sub_grade = 'A3' THEN (${{hiveconf:spark.sql.excellent_rated_pts}} * 0.90)
                WHEN c.grade = 'A' AND c.sub_grade = 'A4' THEN (${{hiveconf:spark.sql.excellent_rated_pts}} * 0.85)
                WHEN c.grade = 'A' AND c.sub_grade = 'A5' THEN (${{hiveconf:spark.sql.excellent_rated_pts}} * 0.80)
                WHEN c.grade = 'B' AND c.sub_grade = 'B1' THEN ${{hiveconf:spark.sql.very_good_rated_pts}}
                WHEN c.grade = 'B' AND c.sub_grade = 'B2' THEN (${{hiveconf:spark.sql.very_good_rated_pts}} * 0.95)
                WHEN c.grade = 'B' AND c.sub_grade = 'B3' THEN (${{hiveconf:spark.sql.very_good_rated_pts}} * 0.90)
                WHEN c.grade = 'B' AND c.sub_grade = 'B4' THEN (${{hiveconf:spark.sql.very_good_rated_pts}} * 0.85)
                WHEN c.grade = 'B' AND c.sub_grade = 'B5' THEN (${{hiveconf:spark.sql.very_good_rated_pts}} * 0.80)
                WHEN c.grade = 'C' AND c.sub_grade = 'C1' THEN ${{hiveconf:spark.sql.good_rated_pts}}
                WHEN c.grade = 'C' AND c.sub_grade = 'C2' THEN (${{hiveconf:spark.sql.good_rated_pts}} * 0.95)
                WHEN c.grade = 'C' AND c.sub_grade = 'C3' THEN (${{hiveconf:spark.sql.good_rated_pts}} * 0.90)
                WHEN c.grade = 'C' AND c.sub_grade = 'C4' THEN (${{hiveconf:spark.sql.good_rated_pts}} * 0.85)
                WHEN c.grade = 'C' AND c.sub_grade = 'C5' THEN (${{hiveconf:spark.sql.good_rated_pts}} * 0.80)
                WHEN c.grade = 'D' AND c.sub_grade = 'D1' THEN ${{hiveconf:spark.sql.bad_rated_pts}}
                WHEN c.grade = 'D' AND c.sub_grade = 'D2' THEN (${{hiveconf:spark.sql.bad_rated_pts}} * 0.95)
                WHEN c.grade = 'D' AND c.sub_grade = 'D3' THEN (${{hiveconf:spark.sql.bad_rated_pts}} * 0.90)
                WHEN c.grade = 'D' AND c.sub_grade = 'D4' THEN (${{hiveconf:spark.sql.bad_rated_pts}} * 0.85)
                WHEN c.grade = 'D' AND c.sub_grade = 'D5' THEN (${{hiveconf:spark.sql.bad_rated_pts}} * 0.80)
                WHEN c.grade = 'E' AND c.sub_grade = 'E1' THEN ${{hiveconf:spark.sql.very_bad_rated_pts}}
                WHEN c.grade = 'E' AND c.sub_grade = 'E2' THEN (${{hiveconf:spark.sql.very_bad_rated_pts}} * 0.95)
                WHEN c.grade = 'E' AND c.sub_grade = 'E3' THEN (${{hiveconf:spark.sql.very_bad_rated_pts}} * 0.90)
                WHEN c.grade = 'E' AND c.sub_grade = 'E4' THEN (${{hiveconf:spark.sql.very_bad_rated_pts}} * 0.85)
                WHEN c.grade = 'E' AND c.sub_grade = 'E5' THEN (${{hiveconf:spark.sql.very_bad_rated_pts}} * 0.80)
                ELSE ${{hiveconf:spark.sql.unacceptable_rated_pts}}
            END
        ) * 0.35) AS financial_health_pts
    FROM {db_prefix}view_staging_ldh_ph_pts ldef
    INNER JOIN {db_prefix}loans l ON ldef.member_id = l.member_id
    INNER JOIN {db_prefix}customers c ON c.member_id = ldef.member_id
    """
    spark.sql(fh_view_query)

def calculate_weekly_loan_scores(spark, database, app_conf):
    """
    Calculates final analytical grades based on the real-time scoring views.
    Returns a DataFrame to the main controller for persistence.
    """
    db_prefix = f"{database}." if database else ""
    realtime_view_df = spark.read.table(f"{db_prefix}view_gold_loan_score_realtime")
    
    return realtime_view_df.withColumn(
        "loan_final_grade",
        F.when(F.col("financial_health_pts") >= float(app_conf["grade.cutoff.very_good"]), "EXCELLENT")
         .when(F.col("financial_health_pts") >= float(app_conf["grade.cutoff.good"]), "GOOD")
         .when(F.col("financial_health_pts") >= float(app_conf["grade.cutoff.bad"]), "MEDIUM")
         .otherwise("HIGH_RISK")
    )