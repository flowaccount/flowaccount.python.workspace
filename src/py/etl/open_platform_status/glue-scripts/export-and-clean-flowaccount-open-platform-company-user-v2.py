import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_unixtime

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ddb_region",
        "ddb_account_id",
        "ddb_table",
        "ddb_role_arn",
        "export_bucket_owner_id",
        "export_bucket",
        "export_prefix",
        "clean_bucket",
        "clean_prefix",
    ],
)
region = args["ddb_region"]
ddb_account_id = args["ddb_account_id"]
ddb_role_arn = args["ddb_role_arn"]
table = args["ddb_table"]
export_bucket_owner_id = args["export_bucket_owner_id"]
export_bucket = args["export_bucket"]
export_prefix = args["export_prefix"]
clean_bucket = args["clean_bucket"]
clean_prefix = args["clean_prefix"]

sc = SparkContext()
glue_context = GlueContext(sc)
logger = glue_context.get_logger()
spark = glue_context.spark_session
job = Job(glue_context)


job.init(args["JOB_NAME"], args)

ddb_connection_options = {
    "dynamodb.export": "ddb",
    "dynamodb.unnestDDBJson": True,
    "dynamodb.tableArn": f"arn:aws:dynamodb:{region}:{ddb_account_id}:table/{table}",
    "dynamodb.s3.bucket": export_bucket,
    "dynamodb.s3.prefix": f"dynamodb/tables/{table}",
    "dynamodb.s3.bucketOwner": export_bucket_owner_id,
}
if len(ddb_role_arn) > 0:
    ddb_connection_options["dynamodb.sts.roleArn"] = ddb_role_arn

unnest_dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options=ddb_connection_options,
    transformation_ctx="DdbTableSource0",
)

unnest_df = unnest_dyf.toDF()
mapped_df1 = unnest_df.withColumn("company_id", col("companyId").cast("long"))
mapped_df2 = mapped_df1.withColumn("shop_id", col("shopId").cast("string"))
mapped_df3 = mapped_df2.withColumn("is_delete", col("isDelete").cast("boolean"))
mapped_df4 = mapped_df3.withColumn("platform_info", col("platformInfo").cast("string"))
mapped_df5 = mapped_df4.withColumn(
    "expired_at", from_unixtime("expiredAt").cast("timestamp")
)
mapped_df6 = mapped_df5.withColumn(
    "payment_channel_id", col("paymentChannelId").cast("long")
)
mapped_df7 = mapped_df6.withColumn(
    "created_at", from_unixtime("createdAt").cast("timestamp")
)
mapped_df8 = mapped_df7.withColumn("expires_in", col("expiresIn").cast("long"))
mapped_df9 = mapped_df8.withColumn("is_vat", col("isVat").cast("boolean"))
# mapped_df = mapped_df.withColumn("payload", col("payload").cast("string"))
# mapped_df = mapped_df.withColumn("guid", col("guid").cast("string"))
mapped_df10 = mapped_df9.withColumn(
    "refresh_expires_in", col("refreshExpiresIn").cast("long")
)
mapped_df11 = mapped_df10.withColumn(
    "updated_at", from_unixtime("updatedAt").cast("timestamp")
)
mapped_df12 = mapped_df11.withColumn("user_id", col("userId").cast("long"))
mapped_df13 = mapped_df12.withColumn(
    "platform_name", col("platformName").cast("string")
)
mapped_df14 = mapped_df13.withColumn(
    "refresh_token", col("refreshToken").cast("string")
)
# mapped_df = mapped_df.withColumn("remarks", col("remarks").cast("string"))
mapped_df15 = mapped_df14.withColumn("access_token", col("accessToken").cast("string"))
# mapped_df = mapped_df.withColumn("email", col("email").cast("string"))
mapped_df16 = mapped_df15.withColumn(
    "disconnect_at", from_unixtime("disconnectAt").cast("timestamp")
)
mapped_df17 = mapped_df16.drop(
    "companyId",
    "shopId",
    "isDelete",
    "platformInfo",
    "expiredAt",
    "paymentChannelId",
    "createdAt",
    "expiresIn",
    "isVat",
    "refreshExpiresIn",
    "updatedAt",
    "userId",
    "platformName",
    "refreshToken",
    "accessToken",
    "disconnectAt",
)
mapped_df = mapped_df17
mapped_dyf = DynamicFrame.fromDF(mapped_df, glue_context, "mapped_dyf")

# Use Spark DataFrame to write because DynamicFrame does not support overwrite
mapped_df.write.mode("overwrite").format("parquet").save(
    f"s3://{clean_bucket}/{clean_prefix}/{table}"
)

job.commit()
