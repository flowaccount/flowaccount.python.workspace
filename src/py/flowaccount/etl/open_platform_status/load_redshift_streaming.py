import pandas as pd


def filter_new_company(cdc_df: pd.DataFrame, company_df: pd.DataFrame) -> pd.DataFrame:
    new_company_df = pd.merge(cdc_df, company_df, how="left", on="company_id")
    new_company_df = new_company_df[new_company_df["company_key"].isna()]
    new_company_df = new_company_df[["company_id"]]
    return new_company_df


def convert_to_fact_table(
    cdc_df: pd.DataFrame, company_df: pd.DataFrame
) -> pd.DataFrame:
    fact_df = pd.merge(cdc_df, company_df, on="company_id", how="inner")

    # Create date key
    fact_df["year"] = fact_df["approximate_creation_date_time"].dt.year
    fact_df["month"] = fact_df["approximate_creation_date_time"].dt.month
    fact_df["day"] = fact_df["approximate_creation_date_time"].dt.day
    fact_df["date_key"] = pd.to_numeric(
        fact_df["year"].astype("str")
        + fact_df["month"].astype("str").str.zfill(2)
        + fact_df["day"].astype("str").str.zfill(2),
        errors="coerce",
    )

    # Create time key
    fact_df["hour"] = fact_df["approximate_creation_date_time"].dt.hour
    fact_df["minute"] = fact_df["approximate_creation_date_time"].dt.minute
    fact_df["second"] = fact_df["approximate_creation_date_time"].dt.second
    fact_df["time_key"] = pd.to_numeric(
        fact_df["hour"].astype("str")
        + fact_df["minute"].astype("str").str.zfill(2)
        + fact_df["second"].astype("str").str.zfill(2),
        errors="coerce",
    )

    # Rename column
    fact_df = fact_df.rename(columns={"platform_name": "platform"})

    # Convert event to status
    fact_df["status"] = fact_df["event_name"].map({"INSERT": True, "REMOVE": False})

    # Drop invalid rows
    fact_df = fact_df.dropna()

    # Form columns
    fact_df = fact_df[["date_key", "time_key", "company_key", "platform", "status"]]
    fact_df = fact_df.astype(
        {
            "date_key": "int",
            "time_key": "int",
            "company_key": "int",
            "platform": "string",
            "status": "boolean",
        }
    )

    return fact_df
