import pandas as pd
import yaml


BIRTHDAY = pd.Timestamp("2021-05-15")


def split_timestamp(df):
    df[["start_date", "start_hour", "start_minute"]] = df.Time.str.split(
        r",|:", expand=True
    )
    df["start_date"] = pd.to_datetime(df.start_date, dayfirst=True)
    return df


def add_time_aggs(df):
    df["month"] = (
        df.start_date.dt.month
        - BIRTHDAY.month
        + (df.start_date.dt.day > BIRTHDAY.day)
        - 1
    ).clip(upper=5)
    df["week"] = (df["start_date"] - BIRTHDAY).dt.days // 7
    return df


def read_file_and_clean(file: str):
    names = ["pumped", "nursed", "nappies", "formula", "sleep", "pump"]
    if file not in names:
        raise ValueError(f"`file` must be one of {names}")

    with open("config.yml") as yml:
        config = yaml.safe_load(yml)

    return (
        pd.read_csv("data/" + config[file]["file_name"])
        .pipe(split_timestamp)
        .rename(columns=config[file].get("rename_columns", {}))
        .drop(columns=["Baby", "Time", "Note"])
        .sort_values(by=["start_date", "start_hour", "start_minute"])
        .reset_index(drop=True)
        .pipe(add_time_aggs)
    )
