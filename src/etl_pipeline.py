import os
import pandas as pd


RAW_PATH = "data/raw/IOT-temp.csv"
PROCESSED_DIR = "data/processed"

CLEANED_PATH = f"{PROCESSED_DIR}/cleaned.csv"
TOP5_HOT_PATH = f"{PROCESSED_DIR}/top5_hot.csv"
TOP5_COLD_PATH = f"{PROCESSED_DIR}/top5_cold.csv"


def extract() -> pd.DataFrame:
    return pd.read_csv(RAW_PATH)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df[df["out/in"] == "In"]

    df["noted_date"] = pd.to_datetime(
        df["noted_date"],
        format="%d-%m-%Y %H:%M",
        errors="raise"
    ).dt.date

    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)

    df["temp"] = df["temp"].clip(lower=p5, upper=p95)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    return df


def aggregate(df: pd.DataFrame) -> None:
    daily_avg = df.groupby("noted_date", as_index=False).agg(avg_temp=("temp", "mean"))

    top5_hot = daily_avg.sort_values("avg_temp", ascending=False).head(5)
    top5_cold = daily_avg.sort_values("avg_temp", ascending=True).head(5)

    top5_hot.to_csv(TOP5_HOT_PATH, index=False)
    top5_cold.to_csv(TOP5_COLD_PATH, index=False)


def load(df: pd.DataFrame) -> None:
    df.to_csv(CLEANED_PATH, index=False)


def run_all() -> None:
    df = extract()
    df = transform(df)
    load(df)
    aggregate(df)


if __name__ == "__main__":
    run_all()
