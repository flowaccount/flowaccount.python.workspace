import awswrangler as wr

clean_bucket = "pipat-clean-bucket"
table_key = "dynamodb/tables/flowaccount-open-platform-company-user-v2"
secret_id = "arn:aws:secretsmanager:ap-southeast-1:697698820969:secret:pipat-etl-redshift-lxoxVP"


def handle(event, context):
    export_id = event["export_id"]

    # Get company ids from the export
    table_df = wr.s3.read_parquet(f"s3://{clean_bucket}/{table_key}", dataset=True)
    table_df = table_df[table_df["export_id"] == export_id]
    table_df = table_df[["company_id"]].drop_duplicates()

    with wr.redshift.connect(secret_id=secret_id, dbname="test") as conn:
        # Select
        redshift_df = wr.redshift.read_sql_query(
            "SELECT dynamodb_key FROM etl.dim_company", con=conn
        )

        # Discover new companies by find companies export table only has
        new_company_df = redshift_df.merge(
            table_df, how="right", left_on="dynamodb_key", right_on="company_id"
        )
        new_company_df = new_company_df[new_company_df["dynamodb_key"].isna()]

        # Transform the DataFrame for insertion
        new_company_df = new_company_df[["company_id"]]
        new_company_df = new_company_df.rename(columns={"company_id": "dynamodb_key"})

        # Write new companies to RedShift
        if not new_company_df.empty:
            wr.redshift.to_sql(
                df=new_company_df,
                table="dim_company",
                schema="etl",
                con=conn,
                mode="append",
                use_column_names=True,
            )

    response = {
        "statusCode": 200,
        "body": {
            "message": "Updated RedShift dim_company",
            "dynamodbExportId": export_id,
        },
    }

    return response
