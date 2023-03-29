from PyEMD import EEMD
import numpy as np
import matplotlib.pyplot as plt

# Define signal
t = np.linspace(0, 1, 200)

sin = lambda x, p: np.sin(2 * np.pi * x * t + p)
S = sin(1, 0)

# Assign EEMD to `eemd` variable
eemd = EEMD()

emd = eemd.EMD
emd.extrema_detection = "parabol"

# Execute EEMD on S
eIMFs = eemd.eemd(S, t)
nIMFs = eIMFs.shape[0]

# Plot results
plt.figure(figsize=(12, 9))
plt.subplot(nIMFs + 1, 1, 1)
plt.plot(t, S, "r")

for n in range(nIMFs):
    plt.subplot(nIMFs + 1, 1, n + 2)
    plt.plot(t, eIMFs[n], "g")
    plt.ylabel("eIMF %i" % (n + 1))
    plt.locator_params(axis="y", nbins=5)

plt.xlabel("Time [s]")
plt.tight_layout()
plt.show()
