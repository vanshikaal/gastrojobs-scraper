"""Synthesises the record-scratch SFX (public/sfx/scratch.wav) with numpy.

Usage: python3 scripts/make-scratch-sfx.py
"""
import os
import wave

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SR = 44100

def lowpass(x, cutoff):
    a = np.exp(-2 * np.pi * cutoff / SR)
    y = np.zeros_like(x)
    acc = 0.0
    # one-pole IIR via cumulative trick in blocks
    for i in range(0, len(x), 4096):
        blk = x[i : i + 4096]
        out = np.empty_like(blk)
        for k, v in enumerate(blk):
            acc = (1 - a) * v + a * acc
            out[k] = acc
        y[i : i + 4096] = out
    return y


def write(path, x):
    x16 = (np.clip(x, -1, 1) * 32767).astype("<i2")
    stereo = np.repeat(x16[:, None], 2, axis=1)
    with wave.open(path, "wb") as w:
        w.setnchannels(2)
        w.setsampwidth(2)
        w.setframerate(SR)
        w.writeframes(stereo.tobytes())


# ---- record scratch --------------------------------------------------------
n = int(0.42 * SR)
t = np.arange(n) / SR
f = np.interp(t, [0, 0.12, 0.2, 0.42], [250, 1400, 300, 120])
ph = 2 * np.pi * np.cumsum(f) / SR
sc = 0.6 * np.sign(np.sin(ph)) * np.exp(-t * 3) + 0.5 * np.random.default_rng(3).uniform(-1, 1, n) * np.exp(-t * 6)
sc = lowpass(sc, 3500)
sc *= 0.9 / np.max(np.abs(sc))
write(os.path.join(ROOT, "public/sfx/scratch.wav"), sc)
print("wrote public/sfx/scratch.wav")
