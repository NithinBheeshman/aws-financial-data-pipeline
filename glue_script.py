
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Bucket names ---
raw_bucket_name = "nith-gwe-datalake-raw-2025"
processed_bucket_name = "nith-gwe-datalake-processed-2025"
# ---------------------------------------------

s3_source_path = f"s3://{raw_bucket_name}/application_train.csv"
s3_destination_path = f"s3://{processed_bucket_name}/processed_loan_data/"

try:
    # --- Step 1: Manually Define the Schema for the columns we need ---
    # This is the blueprint for our data, making the read fast and reliable.
    manual_schema = StructType([
        StructField("SK_ID_CURR", IntegerType(), True),
        StructField("TARGET", IntegerType(), True),
        StructField("NAME_CONTRACT_TYPE", StringType(), True),
        StructField("CODE_GENDER", StringType(), True),
        StructField("FLAG_OWN_CAR", StringType(), True),
        StructField("FLAG_OWN_REALTY", StringType(), True),
        StructField("CNT_CHILDREN", IntegerType(), True),
        StructField("AMT_INCOME_TOTAL", DoubleType(), True),
        StructField("AMT_CREDIT", DoubleType(), True),
        StructField("AMT_ANNUITY", DoubleType(), True),
        StructField("NAME_INCOME_TYPE", StringType(), True),
        StructField("NAME_EDUCATION_TYPE", StringType(), True)
    ])

    # --- Step 2: Read the CSV using the defined schema ---
    # This is the correct, standard Spark method.
    spark_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(manual_schema) \
        .load(s3_source_path)

    print("--- Successfully read data with manual schema. ---")
    
    # --- Step 3: DATA TRANSFORMATIONS ---
    # Fill missing values in the 'AMT_ANNUITY' column
    transformed_df = spark_df.fillna(0, subset=["AMT_ANNUITY"])
    
    print("--- Successfully transformed data. ---")
    
    # --- END OF TRANSFORMATIONS ---

    # Convert back to a Glue DynamicFrame before writing
    final_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "final_dynamic_frame")

    # Write the transformed data to the processed S3 bucket in Parquet format
    glueContext.write_dynamic_frame.from_options(
        frame=final_dynamic_frame,
        connection_type="s3",
        connection_options={"path": s3_destination_path},
        format="parquet",
        transformation_ctx="write_to_s3"
    )
    
    print("--- Successfully wrote data to processed bucket! ---")

finally:
    job.commit()