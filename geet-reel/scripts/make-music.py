"""Original music bed + record-scratch SFX for the reel, synthesised with numpy.

The score follows the edit (reads src/timing.json):
  * playful plucked-uke loop (C-G-Am-F) with bass + shaker through the setup,
  * hard cut to silence on the NOT VERIFIED stamp (crickets carry the freeze),
  * loop returns for the Life Prototyping explanation, with a glockenspiel
    motif added once the LDL idea arrives,
  * over-the-top minor tremolo strings for the "Bollywood plot twist",
  * record scratch -> silence -> gentle uke return and a final resolving chord.

Writes public/music.wav and public/sfx/scratch.wav.
Usage: python3 scripts/make-music.py
"""
import json
import os
import wave

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SR = 44100
T = json.load(open(os.path.join(ROOT, "src/timing.json")))
FPS = T["fps"]
TOTAL = T["durationInFrames"] / FPS
SC = {s["id"]: s for s in T["scenes"]}


def cue(scene, n):
    return [c for c in T["cues"] if c["scene"] == scene][n]


def sec(frames):
    return frames / FPS


# ---- timeline anchors (must mirror the scene code) -------------------------
SLAM = sec(cue("s3", 2)["from"] + 2)  # NOT VERIFIED stamp
S4 = sec(SC["s4"]["from"])
S5 = sec(SC["s5"]["from"])
S6 = sec(SC["s6"]["from"])
INTERRUPT = sec(cue("s6", 2)["from"] - 6)  # record scratch
END = sec(cue("s6", 2)["from"] + 32)  # end card

BPM = 104
BEAT = 60 / BPM
BAR = BEAT * 4


def midi(n):
    return 440.0 * 2 ** ((n - 69) / 12)


def env(n, a=0.005, d=0.4):
    t = np.arange(n) / SR
    e = np.minimum(1, t / a) * np.exp(-t / d)
    return e


def pluck(freq, dur=0.9, bright=0.5):
    """Karplus-Strong string, vectorised one period at a time."""
    n = int(dur * SR)
    p = max(2, int(SR / freq))
    out = np.zeros(n + p + 1)
    rng = np.random.default_rng(int(freq * 100))
    burst = rng.uniform(-1, 1, p)
    burst = np.convolve(burst, [bright, 1 - bright], "same")
    out[:p] = burst
    decay = 0.996
    i = p
    while i < n:
        j = min(i + p, n)
        seg = out[i - p : j - p]
        prev = out[i - p - 1 : j - p - 1] if i - p - 1 >= 0 else np.concatenate([[0], out[i - p : j - p - 1]])
        out[i:j] = decay * 0.5 * (seg + prev[: len(seg)])
        i = j
    return out[:n] * env(n, 0.002, dur * 0.6)


def tone(freq, dur, shape="sine", a=0.01, d=0.5):
    n = int(dur * SR)
    t = np.arange(n) / SR
    if shape == "sine":
        w = np.sin(2 * np.pi * freq * t)
    elif shape == "glock":
        w = np.sin(2 * np.pi * freq * t) + 0.35 * np.sin(2 * np.pi * freq * 2.76 * t) * np.exp(-t * 9)
    else:
        w = np.sin(2 * np.pi * freq * t)
    return w * env(n, a, d)


def saw(freq, n):
    t = np.arange(n) / SR
    return 2 * ((t * freq) % 1.0) - 1


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


mix = np.zeros(int((TOTAL + 2) * SR))


def add(sig, at, gain=1.0):
    i = int(at * SR)
    if i >= len(mix):
        return
    j = min(len(mix), i + len(sig))
    mix[i:j] += sig[: j - i] * gain


CHORDS = {  # uke-ish voicings (midi) + bass root
    "C": ([67, 60, 64, 72], 36),
    "G": ([67, 62, 71, 74], 43),
    "Am": ([69, 60, 64, 69], 45),
    "F": ([69, 60, 65, 72], 41),
}
PROG = ["C", "G", "Am", "F"]
STRUM = [(0, 1.0, "d"), (1, 0.5, "d"), (1.5, 0.6, "u"), (2.5, 0.6, "u"), (3, 0.8, "d"), (3.5, 0.5, "u")]
GLOCK = [(0, 76), (1, 79), (1.5, 81), (2, 79), (3, 76)]
_pluck_cache = {}


def cached_pluck(m):
    if m not in _pluck_cache:
        _pluck_cache[m] = pluck(midi(m), 0.9, 0.55)
    return _pluck_cache[m]


def uke_section(t0, t1, glock=False, gain=1.0):
    bar = 0
    t = t0
    while t < t1:
        name = PROG[bar % 4]
        notes, root = CHORDS[name]
        for beat, vel, direction in STRUM:
            st = t + beat * BEAT
            if st >= t1:
                break
            order = notes if direction == "d" else notes[::-1]
            for k, m in enumerate(order):
                add(cached_pluck(m), st + k * 0.012, 0.16 * vel * gain)
            # shaker on off-beats
            if beat % 1 == 0.5:
                n = int(0.06 * SR)
                add(np.random.default_rng(int(st * 1000)).uniform(-1, 1, n) * env(n, 0.002, 0.02), st, 0.05 * gain)
        for beat in (0, 2):
            st = t + beat * BEAT
            if st < t1:
                add(tone(midi(root), BEAT * 1.8, "sine", 0.01, 0.5), st, 0.32 * gain)
        if glock:
            for beat, m in GLOCK:
                st = t + beat * BEAT
                if st < t1:
                    add(tone(midi(m + (0 if bar % 2 == 0 else -2)), 1.2, "glock", 0.002, 0.35), st, 0.12 * gain)
        bar += 1
        t += BAR


def drama(t0, t1):
    """Tremolo minor strings that swell toward the plot twist."""
    prog = [([57, 60, 64, 69], 2.0), ([53, 57, 60, 65], 1.6), ([52, 56, 59, 64], 10.0)]  # Am, F, E
    t = t0
    for notes, length in prog:
        dur = min(length, t1 - t)
        if dur <= 0:
            break
        n = int(dur * SR)
        tt = np.arange(n) / SR
        sig = np.zeros(n)
        for m in notes:
            for det in (-0.12, 0.12):
                sig += saw(midi(m) * 2 ** (det / 12), n)
        trem = 0.6 + 0.4 * np.sin(2 * np.pi * 9 * tt)
        swell = np.minimum(1, tt / 0.25)
        sig = sig * trem * swell / len(notes)
        add(sig, t, 0.05)
        add(tone(midi(notes[0] - 24), dur, "sine", 0.02, dur), t, 0.35)
        t += dur


# ---- arrange ---------------------------------------------------------------
uke_section(0.0, SLAM - 0.02)                 # setup: s1-s3 until the stamp
uke_section(S4, S5, glock=False)              # concept
uke_section(S5, S6, glock=True)               # LDL Geet
drama(S6, INTERRUPT)                          # Bollywood melodrama
uke_section(INTERRUPT + 0.45, TOTAL - 1.4, glock=True, gain=0.8)  # calm return
# final resolving C chord + glock sparkle
for k, m in enumerate([60, 64, 67, 72]):
    add(pluck(midi(m), 2.2, 0.5), TOTAL - 1.4 + k * 0.03, 0.2)
add(tone(midi(36), 1.6, "sine", 0.01, 0.8), TOTAL - 1.4, 0.35)
add(tone(midi(84), 1.4, "glock", 0.002, 0.5), TOTAL - 1.3, 0.15)

# hard cuts: zero everything after each cut point until the next section
def cut(a, b):
    mix[int(a * SR) : int(b * SR)] = 0


cut(SLAM, S4)
cut(INTERRUPT, INTERRUPT + 0.45)

# short fades at cut edges to avoid clicks
for edge in (SLAM, INTERRUPT):
    i = int(edge * SR)
    f = int(0.012 * SR)
    mix[i - f : i] *= np.linspace(1, 0, f)

mix = mix[: int(TOTAL * SR)]
mix = np.tanh(mix * 1.2) / np.tanh(1.2)
mix *= 0.8 / max(1e-6, np.max(np.abs(mix)))


def write(path, x):
    x16 = (np.clip(x, -1, 1) * 32767).astype("<i2")
    stereo = np.repeat(x16[:, None], 2, axis=1)
    with wave.open(path, "wb") as w:
        w.setnchannels(2)
        w.setsampwidth(2)
        w.setframerate(SR)
        w.writeframes(stereo.tobytes())


write(os.path.join(ROOT, "public/music.wav"), mix)

# ---- record scratch --------------------------------------------------------
n = int(0.42 * SR)
t = np.arange(n) / SR
f = np.interp(t, [0, 0.12, 0.2, 0.42], [250, 1400, 300, 120])
ph = 2 * np.pi * np.cumsum(f) / SR
sc = 0.6 * np.sign(np.sin(ph)) * np.exp(-t * 3) + 0.5 * np.random.default_rng(3).uniform(-1, 1, n) * np.exp(-t * 6)
sc = lowpass(sc, 3500)
sc *= 0.9 / np.max(np.abs(sc))
write(os.path.join(ROOT, "public/sfx/scratch.wav"), sc)
print(f"music {len(mix)/SR:.2f}s; SLAM {SLAM:.2f}s, S4 {S4:.2f}s, S6 {S6:.2f}s, INTERRUPT {INTERRUPT:.2f}s, END {END:.2f}s")
