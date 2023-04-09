import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests


class Data:
    folder: Path = Path(f"data/raw/BTCUSDC/5m/")

    @staticmethod
    def get_raw(days: int, coin: str = "BTCUSDC", interval: str = "5m"):
        Data.folder = Path(f"data/raw/{coin}/{interval}/")
        Data.folder.mkdir(parents=True, exist_ok=True)
        base_url = "https://data.binance.vision/data/spot/daily/klines"

        for i in range(365, days + 1):
            Data.download_file(
                f"{base_url}/{coin}/{interval}/{coin}-{interval}-{(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')}.zip",
            )

    @staticmethod
    def download_file(url: str):
        file_name = url.split("/")[-1]
        time.sleep(1)

        with requests.get(url, stream=True) as r, open(
            Data.folder / file_name, "wb"
        ) as f:
            if r.status_code == 404:
                url = url.replace("BTCUSDC", "BTCBUSD")
                r = requests.get(url, stream=True)
            for chunk in r.iter_content():
                f.write(chunk)

    @staticmethod
    def zip2csv():
        for file_name in Data.folder.glob("*.zip"):
            try:
                with zipfile.ZipFile(file_name, "r") as zip_ref:
                    zip_ref.extractall(Data.folder)
                file_name.unlink()
            except:
                print(file_name)

    @staticmethod
    def concat_csv():
        files = Data.folder.glob("*.csv")
        columns = [
            "Id",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close_time",
            "STX",
            "Number_trades",
            "Taker_buy_base_asset_volume",
            "Taker_buy_STX",
            "Ignore",
        ]
        main_df = pd.concat(
            (pd.read_csv(f, names=columns) for f in files),
            ignore_index=True,
        )
        main_df.columns = columns
        Data.folder = Path(str(Data.folder).replace("raw", "interim"))
        Data.folder.mkdir(parents=True, exist_ok=True)
        main_df.to_csv(Data.folder / "data_concat.csv", index=False)

    @staticmethod
    def process_data():
        Data.folder = Path(str(Data.folder).replace("raw", "interim"))
        data = pd.read_csv(Data.folder / "data_concat.csv")
        close_data = data.loc[:, ["Close_time", "Close"]]
        close_data["Close_time"] = pd.to_datetime(data["Close_time"], unit="ms")
        close_data.sort_values("Close_time", inplace=True)
        close_data = close_data.set_index("Close_time")

        Data.folder = Path(str(Data.folder).replace("interim", "processed"))
        Data.folder.mkdir(parents=True, exist_ok=True)
        close_data.to_csv(Data.folder / "data_processed.csv")


# Data.get_raw(882)
# Data.zip2csv()
# Data.concat_csv()
Data.process_data()
