import pandas as pd
import yaml


BIRTHDAY = pd.Timestamp("2021-05-15")


def split_timestamp(df):
    df[["start_date", "start_hour", "start_minute"]] = df.Time.str.split(
        r",|:", expand=True
    )
    df["start_date"] = pd.to_datetime(df.start_date, dayfirst=True)
    return df


def add_time_aggs(df, date_col="start_date"):
    df[date_col] = pd.to_datetime(df[date_col])
    df["month"] = (
        df[date_col].dt.month
        - BIRTHDAY.month
        + (df[date_col].dt.day > BIRTHDAY.day)
        - 1
    ).clip(upper=5)
    df["week"] = (df[date_col] - BIRTHDAY).dt.days // 7
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


def get_sleep_by_minute():
    sleep = read_file_and_clean("sleep").assign(
        timestamp=lambda x: pd.to_datetime(
            x["start_date"].dt.strftime("%Y-%m-%d")
            + x["start_hour"]
            + x["start_minute"]
        ),
        status="asleep",
    )

    full_schedule = pd.concat(
        [
            pd.DataFrame(
                {
                    "timestamp": sleep.timestamp + pd.to_timedelta(sleep.duration, "m"),
                    "status": "awake",
                }
            ),
            sleep[["timestamp", "status"]],
        ]
    ).sort_values(by="timestamp")

    lifespan = pd.Series(
        pd.date_range(
            sleep.timestamp.min(), full_schedule.timestamp.max(), freq="1min"
        ),
        name="minute",
    )

    sleep_by_minute = (
        pd.merge_asof(lifespan, full_schedule, right_on="timestamp", left_on="minute")
        .assign(
            date=lambda x: x["minute"].astype(str).str.split(" ", expand=True)[0],
            time=lambda x: x["minute"].astype(str).str.split(" ", expand=True)[1],
            awake=lambda x: (x["status"] == "awake").astype(float),
        )
        .drop(columns=["timestamp"])
    )

    return sleep_by_minute


def get_sleep_by_night():
    sleep = get_sleep_by_minute().assign(
        night=lambda x: ~x["time"].between("07:00:00", "19:00:00")
    )

    night_stamp = (
        sleep.loc[sleep["time"] == "19:00:00", ["minute", "date"]]
        .rename(columns={"minute": "night_start", "date": "night_date"})
        .reset_index(drop=True)
    )

    night_minutes = pd.merge_asof(
        sleep, night_stamp, left_on="minute", right_on="night_start"
    ).assign(asleep=lambda x: 1 - x["awake"])

    nights = (
        (
            night_minutes[night_minutes["night"]].groupby("night_date").sum()["asleep"]
            / 60
        )
        .to_frame()
        .reset_index()
        .pipe(add_time_aggs, "night_date")
    )

    return nights


def check_overlap(sleep):
    sleep = sleep.assign(
        timestamp=lambda x: pd.to_datetime(
            x["start_date"].dt.strftime("%Y-%m-%d")
            + x["start_hour"]
            + x["start_minute"]
        ),
        status="asleep",
    )
    sleep["end"] = sleep.timestamp + pd.to_timedelta(sleep.duration, "m")
    sleep["next_start"] = sleep["timestamp"].shift(-1)

    overlap = sleep[sleep.end >= sleep.next_start]

    if len(overlap) == 0:
        print("no overlapping sleep sessions")
        return None
    else:
        return overlap
