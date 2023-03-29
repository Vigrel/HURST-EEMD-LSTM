import concurrent.futures
import os
import zipfile
from datetime import datetime, timedelta

import requests


class Data:
    @staticmethod
    def get_raw(days: int, coin: str = "BTCUSDC", interval: str = "5m"):
        folder = f"data/raw/{coin}/{interval}/"
        os.makedirs(folder, exist_ok=True)
        base_url = "https://data.binance.vision/data/spot/daily/klines"
        downloaded_files = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    Data.download_file,
                    f"{base_url}/{coin}/{interval}/{coin}-{interval}-{(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')}.zip",
                    folder,
                )
                for i in range(1, days + 1)
            ]

            for future in concurrent.futures.as_completed(futures):
                downloaded_files.append(future.result())

        with open(f"{folder}/downloaded_files.txt", "w") as output:
            output.write("\n".join(downloaded_files))

    @staticmethod
    def download_file(url: str, folder: str) -> str:
        file_name = url.split("/")[-1]
        with requests.get(url, stream=True) as r, open(
            f"{folder}/{file_name}", "wb"
        ) as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        with zipfile.ZipFile(f"{folder}/{file_name}", "r") as zip_ref:
            zip_ref.extract(f"{file_name[:-4]}.csv", folder)
        os.remove(f"{folder}/{file_name}")
        return file_name