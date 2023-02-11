import os
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True)
def fetch_data(data_url: str, folder: str) -> None:
    os.system(f"wget {data_url} -P {folder}")


@task(log_prints=True)
def write_gcs(local_path: Path, gc_path: Path) -> None:
    gcs_block = GcsBucket.load("dtc-gcs-bucket")
    gcs_block.upload_from_path(from_path=local_path, to_path=gc_path)
    return


@flow(log_prints=True)
def main_flow(months: list) -> None:

    for month in months:
        path = Path(__file__).parent
        folder = f"{path}/fhv_ny_taxi_data/"

        file = f"fhv_tripdata_2019-{month:0>2}.csv.gz"
        data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file}"

        local_path = Path(f"{folder}/{file}")
        gcs_path = Path(f"./FHV NY Taxi data 2019/{file}")

        fetch_data(data_url, folder)
        write_gcs(local_path, gcs_path)


if __name__ == "__main__":
    months = list(range(1, 13))
    main_flow(months)
