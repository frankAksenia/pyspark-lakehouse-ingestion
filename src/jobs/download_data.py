from urllib.request import urlretrieve
from src.utils.paths import DATA_RAW, TRIPS_RAW

# https://www.kaggle.com/datasets/shayanshahid997/yellow-taxi-trip-record-of-january-2024
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    if TRIPS_RAW.exists():
        print(f"Already exists: {TRIPS_RAW}")
        return
    print(f"Downloading {URL}")
    urlretrieve(URL, TRIPS_RAW)
    print(f"Saved to {TRIPS_RAW}")


if __name__ == "__main__":
    main()
