import os
import pandas as pd
from prefect import task, flow
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket


def fetch(url: str) -> pd.DataFrame:
    return pd.read_csv(url, engine="pyarrow")


@task(log_prints=True, retries=3)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    data = df.astype(
        {"PUlocationID": "Int64", "DOlocationID": "Int64", "SR_Flag": "Int64"}
    )
    print(f"columns: {data.dtypes}")
    print(f"rows: {len(data)}")
    return data


@task(log_prints=True)
def write_local(df: pd.DataFrame, file: str) -> Path:
    path = Path(__file__).parent
    folder = f"{path}/FHV NY Taxi data 2019"

    if not os.path.exists(folder):
        os.makedirs(folder)

    local_path = Path(f"{path}/FHV NY Taxi data 2019/{file}.csv.gz")
    gcs_path = Path(f"./FHV NY Taxi data 2019/{file}.csv.gz")

    print(f"Here are the dtypes : {df.dtypes}")
    # df = df.to_parquet(local_path, compression="gzip")
    return [local_path, gcs_path]


@task(log_prints=True)
def write_gcs(local_path: Path, gc_path: Path) -> None:
    gcs_block = GcsBucket.load("dtc-gcs-bucket")
    gcs_block.upload_from_path(from_path=local_path, to_path=gc_path)
    return


@flow(log_prints=True)
def main_flow(months: list) -> None:
    for month in months:

        file = f"fhv_tripdata_2019-{month:02}"
        data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file}.csv.gz"

        raw = fetch(data_url)
        cleaned = transform(raw)
        local = write_local(cleaned, file)
        local_path = local[0]
        gcs_path = local[1]
        write_gcs(local_path, gcs_path)


if __name__ == "__main__":
    months = list(range(2, 3))
    main_flow(months)
