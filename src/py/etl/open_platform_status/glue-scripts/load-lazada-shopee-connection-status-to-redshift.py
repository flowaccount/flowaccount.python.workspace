import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "catalog_db",
        "catalog_table",
        "redshift_connection",
        "staging_db",
        "staging_schema",
        "staging_table",
        "staging_temp_table",
    ],
)
catalog_db = args["catalog_db"]
catalog_table = args["catalog_table"]
redshift_connection = args["redshift_connection"]
staging_db = args["staging_db"]
staging_schema = args["staging_schema"]
staging_table = args["staging_table"]
staging_temp_table = args["staging_temp_table"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_table,
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Select Fields
SelectFields_node1650887060855 = SelectFields.apply(
    frame=S3bucket_node1,
    paths=["company_id", "is_delete", "platform_name"],
    transformation_ctx="SelectFields_node1650887060855",
)

# Script generated for node Pivot Connection
SqlQuery0 = """
SELECT
    company_id AS dynamodb_key,
    COUNT(
        CASE WHEN platform_name = 'lazada' AND is_delete = FALSE THEN 1
             ELSE NULL END) > 0 AS has_lazada_connection_new,
    COUNT(
        CASE WHEN platform_name = 'shopee' AND is_delete = FALSE THEN 1
             ELSE NULL END) > 0 AS has_shopee_connection_new
FROM myDataSource
GROUP BY dynamodb_key

"""
PivotConnection_node1650942948977 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": SelectFields_node1650887060855},
    transformation_ctx="PivotConnection_node1650942948977",
)

# Script generated for node Staging Connection
pre_query = f"""
    DROP TABLE IF EXISTS {staging_schema}.{staging_temp_table};
    CREATE TABLE {staging_schema}.{staging_temp_table}
    AS SELECT * FROM {staging_schema}.{staging_table} WHERE 1=2;
"""
post_query = f"""
    BEGIN;
    
    DELETE FROM {staging_schema}.{staging_table}
    USING {staging_schema}.{staging_temp_table}
    WHERE {staging_schema}.{staging_temp_table}.dynamodb_key = {staging_schema}.{staging_table};
    
    INSERT INTO {staging_schema}.{staging_table}
    SELECT * FROM {staging_schema}.{staging_temp_table};
    
    DROP TABLE {staging_schema}.{staging_temp_table};
    
    END;
"""
StagingConnection_node1650949619208 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=PivotConnection_node1650942948977,
    catalog_connection=redshift_connection,
    connection_options={
        "database": staging_db,
        "dbtable": f"{staging_schema}.{staging_temp_table}",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="StagingConnection_node1650949619208",
)

job.commit()
