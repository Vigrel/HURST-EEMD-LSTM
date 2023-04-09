import pandas as pd
import matplotlib.pyplot as plt
from PyEMD import EEMD
import numpy as np

data = pd.read_csv("data/processed/BTCUSDC/5m/data_processed.csv")
signal = data["Close"].to_numpy()

eemd = EEMD()
IMFs = eemd(signal)
np.save("data/processed/IMFs.npy", IMFs)

plt.figure(figsize=(25, 10))

for i, IMF in enumerate(IMFs):
    plt.subplot(len(IMFs), 1, i + 1)
    plt.plot(IMF, label="IMF {}".format(i + 1))
    plt.legend()
plt.savefig("reports/IMFs.png")
