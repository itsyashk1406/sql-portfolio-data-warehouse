import sys
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------- Glue Job Setup ----------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------- Read Bronze tables ----------
crm_cust_info     = glueContext.create_dynamic_frame.from_catalog(database="bronze_db", table_name="cust_info").toDF()
crm_prd_info      = glueContext.create_dynamic_frame.from_catalog(database="bronze_db", table_name="prd_info").toDF()
crm_sales_details = glueContext.create_dynamic_frame.from_catalog(database="bronze_db", table_name="sales_details").toDF()
erp_cust_az12     = glueContext.create_dynamic_frame.from_catalog(database="bronze_db", table_name="cust_az12").toDF()
erp_loc_a101      = glueContext.create_dynamic_frame.from_catalog(database="bronze_db", table_name="loc_a101").toDF()
erp_px_cat_g1v2   = glueContext.create_dynamic_frame.from_catalog(database="bronze_db", table_name="px_cat_g1v2").toDF()

# ---------- CRM Transformations ----------

# crm_cust_info: deduplication + trim + standardization
crm_cust_info = crm_cust_info.withColumn("cst_create_date", F.col("cst_create_date").cast("date"))
window_cust = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())

silver_crm_cust_info = (
    crm_cust_info
    .withColumn("row_num", F.row_number().over(window_cust))
    .filter(F.col("row_num") == 1).drop("row_num")
    .withColumn("cst_gndr",
        F.when(F.upper(F.trim(F.col("cst_gndr"))) == "M", "Male")
         .when(F.upper(F.trim(F.col("cst_gndr"))) == "F", "Female")
         .otherwise("n/a")
    )
    .withColumn("cst_marital_status",
        F.when(F.upper(F.trim(F.col("cst_marital_status"))) == "M", "Married")
         .when(F.upper(F.trim(F.col("cst_marital_status"))) == "S", "Single")
         .otherwise("n/a")
    )
    .withColumn("cst_key", F.trim(F.col("cst_key")))
    .withColumn("cst_firstname", F.trim(F.col("cst_firstname")))
    .withColumn("cst_lastname", F.trim(F.col("cst_lastname")))
    .filter(F.col("cst_id").isNotNull())
)

# crm_prd_info: trim, substring, standardization, start/end date logic
# Ensure we consistently use prd_start_dt
window_prd = Window.partitionBy("prd_key").orderBy("prd_start_dt")

silver_crm_prd_info = (
    crm_prd_info
    .withColumn("cat_id", F.regexp_replace(F.substring(F.col("prd_key"), 1, 5), "-", "_"))
    .withColumn("prd_key", F.expr("substring(prd_key, 7, length(prd_key))"))
    .withColumn("prd_nm", F.trim(F.col("prd_nm")))
    .withColumn("prd_cost", F.when(F.col("prd_cost").isNull(), F.lit(0)).otherwise(F.col("prd_cost")))
    .withColumn("prd_line",
        F.when(F.upper(F.trim(F.col("prd_line"))) == 'M', 'Mountain')
         .when(F.upper(F.trim(F.col("prd_line"))) == 'R', 'Road')
         .when(F.upper(F.trim(F.col("prd_line"))) == 'S', 'Other Sales')
         .when(F.upper(F.trim(F.col("prd_line"))) == 'T', 'Touring')
         .otherwise('n/a')
    )
    .withColumn("prd_start_dt", F.col("prd_start_dt").cast("date"))
    .withColumn("next_start_dt", F.lead("prd_start_dt", 1).over(window_prd))
    .withColumn("prd_end_dt", F.expr("date_sub(next_start_dt, 1)"))
    .drop("next_start_dt")
)

# Safe parser for YYYYMMDD-like integers/strings -> date
def parse_date_strict(col_name):
    s = F.col(col_name).cast("string")
    s = F.lpad(s, 8, "0")  # pad to 8 if needed
    valid = (
        (F.col(col_name) != F.lit(0)) &
        (F.length(s) == 8) &
        (s >= F.lit("19000101")) &
        (s <= F.lit("20500101"))
    )
    return F.when(valid, F.to_date(s, "yyyyMMdd")).otherwise(F.lit(None).cast("date"))

silver_crm_sales_details = (
    crm_sales_details
    .withColumn("sls_order_dt", parse_date_strict("sls_order_dt"))
    .withColumn("sls_ship_dt",  parse_date_strict("sls_ship_dt"))
    .withColumn("sls_due_dt",   parse_date_strict("sls_due_dt"))
    .withColumn("sls_sales", F.when(
        (F.col("sls_sales") <= 0) | F.col("sls_sales").isNull() |
        (F.col("sls_sales") != F.col("sls_quantity") * F.col("sls_price")),
        F.abs(F.col("sls_quantity") * F.col("sls_price"))
    ).otherwise(F.col("sls_sales")))
    .withColumn("sls_quantity", F.when(
        (F.col("sls_quantity") <= 0) | F.col("sls_quantity").isNull(),
        F.when(F.col("sls_price") == 0, F.lit(None).cast("double"))
         .otherwise(F.abs(F.col("sls_sales") / F.col("sls_price")))
    ).otherwise(F.col("sls_quantity")))
    .withColumn("sls_price", F.when(
        (F.col("sls_price") <= 0) | F.col("sls_price").isNull(),
        F.when(F.col("sls_quantity") == 0, F.lit(None).cast("double"))
         .otherwise(F.abs(F.col("sls_sales") / F.col("sls_quantity")))
    ).otherwise(F.col("sls_price")))
)

# ---------- ERP Transformations ----------

silver_erp_cust_az12 = (
    erp_cust_az12
    # cast bdate once, then null out future
    .withColumn("bdate", F.col("bdate").cast("date"))
    .withColumn(
        "cid",
        F.when(F.col("cid").startswith("NAS"), F.expr("substring(cid, 4, length(cid))"))
         .otherwise(F.col("cid"))
    )
    .withColumn(
        "bdate",
        F.when(F.col("bdate") > F.current_date(), F.lit(None).cast("date"))
         .otherwise(F.col("bdate"))
    )
    .withColumn(
        "gen",
        F.when(F.upper(F.trim(F.col("gen"))).isin("M", "MALE"), "Male")
         .when(F.upper(F.trim(F.col("gen"))).isin("F", "FEMALE"), "Female")
         .otherwise("n/a")
    )
)

silver_erp_loc_a101 = (
    erp_loc_a101
    .withColumn("cid", F.regexp_replace(F.col("cid"), "-", ""))
    .withColumn(
        "cntry",
        F.when(F.trim(F.col("cntry")).isin("US", "USA"), "United States")
         .when(F.trim(F.col("cntry")) == "DE", "Germany")
         .when((F.trim(F.col("cntry")) == "") | (F.col("cntry").isNull()), "n/a")
         .otherwise(F.trim(F.col("cntry")))
    )
)

silver_erp_px_cat_g1v2 = (
    erp_px_cat_g1v2
    .select(
        F.trim(F.col("id")).alias("id"),
        F.trim(F.col("cat")).alias("cat"),
        F.trim(F.col("subcat")).alias("subcat"),
        F.trim(F.col("maintenance")).alias("maintenance")
    )
)

# ---------- Write Silver Tables to S3 ----------
tables_to_write = {
    "crm_cust_info":     silver_crm_cust_info,
    "crm_prd_info":      silver_crm_prd_info,
    "crm_sales_details": silver_crm_sales_details,
    "erp_cust_az12":     silver_erp_cust_az12,
    "erp_loc_a101":      silver_erp_loc_a101,
    "erp_px_cat_g1v2":   silver_erp_px_cat_g1v2
}


bucket_name = "datawarehouse-portfolio-project"
prefix = "silver/"

s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
bucket.objects.filter(Prefix=prefix).delete()
print("✅ Silver S3 folder cleared")


glue = boto3.client("glue")
db_name = "silver_db"

next_token = None
while True:
    if next_token:
        resp = glue.get_tables(DatabaseName=db_name, NextToken=next_token)
    else:
        resp = glue.get_tables(DatabaseName=db_name)
    for tbl in resp.get("TableList", []):
        glue.delete_table(DatabaseName=db_name, Name=tbl["Name"])
    next_token = resp.get("NextToken")
    if not next_token:
        break

print("✅ All tables deleted from silver_db")

# ---------- Write and Register in Glue Catalog ----------
for table_name, df in tables_to_write.items():
    print(f"Processing table: {table_name}")

    # Convert to DynamicFrame
    dynamic_df = DynamicFrame.fromDF(df, glueContext, table_name)

    # Define S3 path
    s3_path = f"s3://{bucket_name}/silver/{table_name}"

    # Use Glue Sink to both write to S3 & update Glue Catalog
    sink = glueContext.getSink(
        path=s3_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx=f"sink_{table_name}"
    )

    sink.setCatalogInfo(catalogDatabase=db_name, catalogTableName=table_name)
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.writeFrame(dynamic_df)

job.commit()
print("Silver Layer ETL Completed Successfully!")
