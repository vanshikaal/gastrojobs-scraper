# Geet Needed Life Prototyping — 43 s vertical reel

An animated Instagram reel built in [Remotion](https://remotion.dev) (React).
It runs 1080×1920 at 30 fps: 1290 frames, exactly 43.0 s.

## Storyboard → code

| Time | Frames | Scene file | What happens |
|---|---|---|---|
| 0–4 s | 0–119 | `src/scenes/S1Train.tsx` | Geet sprints for the train, energy meters (100% vs 3%), leaps, then a freeze frame with the title |
| 4–11 s | 120–329 | `src/scenes/S2Dream.tsx` | Thought bubble with wedding bells, a mandap, a house and FOREVER. A giant stamp lands APPROVED, and a sticky note reads "approved by: Geet / reviewed by: Geet" |
| 11–17 s | 330–509 | `src/scenes/S3Hypothesis.tsx` | Anshuman on a HYPOTHESIS #01 index card, then a NOT VERIFIED stamp. Freeze: PAUSED, *cricket noises*, Geet peeks up in horror |
| 17–25 s | 510–749 | `src/scenes/S4Paths.tsx` | Split screen. COMMIT is one giant leap (she falls in). PROTOTYPE is TEST → LEARN → DECIDE stones (she arrives) |
| 25–36 s | 750–1079 | `src/scenes/S5LDL.tsx` | Plan A "run away at night" gets a NOPE stamp. Portal to the LDL universe, where LDL Geet has tiny glasses and a clipboard. Then a split-screen call, "Visit ≠ Move in" with a return ticket, and the plan updated to v2 |
| 36–43 s | 1080–1289 | `src/scenes/S6Ending.tsx` | Both Geets side by side (oversized heart vs tiny checklist). End card: "Prototype first. Commit better." |

Shared pieces:
- `src/components/Geet.tsx` and `Anshuman.tsx`: rigged vector characters. Arm and leg joint angles, expressions, squash and stretch, dupatta flutter.
- `src/components/Doodles.tsx`: arrows that draw on, rubber stamps, hearts, bubbles, sparkles, strike-throughs.
- `src/components/Defs.tsx`: paper texture, floral salwar print, and the "line boil" filter that gives the hand-drawn jitter.
- `src/components/Subtitles.tsx`: burned-in captions inside the mobile-safe zone (y 1395–1560).

## Voiceover and subtitles

`scripts/script.json` holds the exact narration, split into subtitle cues per scene.
`node scripts/build-timing.mjs --scratch-vo` does three things:
- writes `src/timing.json` (the cue frames that drive subtitles and some animation beats)
- writes `subtitles.srt`
- writes `public/vo-scratch.wav`

The scratch voice is an offline espeak-ng/MBROLA voice, for timing only. It sounds robotic and is **not** a final voiceover.

To add a real voiceover:
1. Record or generate it to match the cue times in `subtitles.srt`. Each line starts at its cue time.
2. Save it as `public/vo-final.wav`, 43 s long.
3. Render with `--props='{"voiceover":"final","sfx":true,"subtitles":true}'`.

`voiceover: "none"` renders subtitles and SFX only. The SFX (whoosh, pop, stamp thump, bell, ding, crickets) are synthesised with ffmpeg into `public/sfx/`.

## Commands

```bash
npm install
npm run studio          # live preview / editing
npx remotion render GeetReel out/reel.mp4 --crf=18 --props='{"voiceover":"scratch","sfx":true,"subtitles":true}'
```

In a sandbox without Remotion's own Chrome download, add
`--browser-executable=/path/to/chrome-headless-shell --timeout=180000`.
The paper/boil SVG filters make each frame fairly heavy, so the longer timeout helps.
